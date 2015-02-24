-module(freya_rollup).
-behaviour(gen_server).

-include("freya.hrl").
-include("freya_metrics.hrl").

%% API
-export([replicas/0, read_consistency/0, write_consistency/0,
         edge_arrival_latency/0, vnode_arrival_latency/0]).
-export([push/5, push/6]).
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(ROLLUP_GROUP(Key), {rollup, Key}).
-define(DEFAULT_EDGE_ARRIVAL_LATENCY, timer:minutes(5)).
-define(DEFAULT_VNODE_ARRIVAL_LATENCY, timer:seconds(10)).

%%%===================================================================
%%% API
%%%===================================================================

push(Metric, Tags, Ts, Value, Aggregate) ->
    MaxDelay = edge_arrival_latency(),
    push(Metric, Tags, Ts, Value, Aggregate, MaxDelay).

push(Metric, Tags, Ts, Value, {_Fun, Precision}=Aggregate, MaxDelay) ->
    case can_aggregate(Ts, Precision, MaxDelay) of
        true ->
            do_push(Metric, Tags, Ts, Value, Aggregate);
        false ->
            quintana:notify_spiral({?Q_EDGE_OUTDATED, 1}),
            {error, too_late}
    end.

replicas() ->
    {ok, N} = freya:get_env(rollup_replicas),
    N.

read_consistency() ->
    {ok, R} = freya:get_env(rollup_read_consistency),
    rollup_consistency(R).

write_consistency() ->
    {ok, W} = freya:get_env(rollup_write_consistency),
    rollup_consistency(W).

edge_arrival_latency() ->
    freya:get_env(rollup_edge_arrival_latency, ?DEFAULT_EDGE_ARRIVAL_LATENCY).

vnode_arrival_latency() ->
    freya:get_env(rollup_vnode_arrival_latency, ?DEFAULT_VNODE_ARRIVAL_LATENCY).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create(Metric, Tags, Ts, Aggregate) ->
    gen_server:call(?MODULE, {create, Metric, Tags, Ts, Aggregate}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({create, Metric, Tags, Ts, Aggregate}, _From, State) ->
    Key = ?ROLLUP_GROUP(freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate)),
    case find_worker_process(Key) of
        {ok, _} = Ok ->
            {reply, Ok, State};
        {error, not_found} ->
            {ok, Pid} = freya_rollup_sup:start_child([Metric, Tags, Ts, Aggregate]),
            ok = pg2:create(Key),
            ok = pg2:join(Key, Pid),
            {reply, {ok, Pid}, State}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
can_aggregate(_Ts, _Precision, infinity) ->
    true;
can_aggregate(Ts, Precision, MaxDelay) ->
    UpperTs    = freya_utils:ceil(Ts, Precision),
    Now        = tic:now_to_epoch_msecs(),
    Now =< UpperTs + MaxDelay.

do_push(Metric, Tags, Ts, Value, {_Fun, Precision}=Aggregate) ->
    AlignedTs       = freya_utils:floor(Ts, Precision),
    SanitizedTags   = freya_utils:sanitize_tags(Tags),
    Key             = ?ROLLUP_GROUP(
                         freya_utils:aggregate_key(Metric, SanitizedTags,
                                                   AlignedTs, Aggregate)),
    {ok, Pid} = case find_worker_process(Key) of
                    {ok, _} = Ok ->
                        Ok;
                    {error, not_found} ->
                        create(Metric, SanitizedTags, AlignedTs, Aggregate)
                end,
    freya_rollup_wrk:push(Pid, Value).

find_worker_process(Key) ->
    case pg2:get_local_members(Key) of
        [] ->
            {error, not_found};
        [Pid] ->
            {ok, Pid};
        {error, {no_such_group, _}} ->
            {error, not_found}
    end.

rollup_consistency(quorum) ->
    N = replicas(),
    N div 2 + 1;
rollup_consistency(V) when is_integer(V) ->
    V.
