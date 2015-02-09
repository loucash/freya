-module(freya_snapshot).
-behaviour(gen_server).

-include("freya.hrl").

%% API
-export([snapshot/4]).
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-define(SNAPSHOT_GROUP(Key), {snapshot, Key}).

%%%===================================================================
%%% API
%%%===================================================================

snapshot(Metric, Tags, Ts, Aggregate) ->
    Key = ?SNAPSHOT_GROUP(freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate)),
    case find_worker_process(Key) of
        {ok, _} ->
            {error, already_exists};
        {error, not_found} ->
            {ok, _} = run(Metric, Tags, Ts, Aggregate),
            ok
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

run(Metric, Tags, Ts, Aggregate) ->
    gen_server:call(?MODULE, {run, Metric, Tags, Ts, Aggregate}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({run, Metric, Tags, Ts, Aggregate}, _From, State) ->
    Key = ?SNAPSHOT_GROUP(freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate)),
    case find_worker_process(Key) of
        {ok, _} = Ok ->
            {reply, Ok, State};
        {error, not_found} ->
            {ok, Pid} = freya_snapshot_sup:start_child([Metric, Tags, Ts, Aggregate]),
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
find_worker_process(Key) ->
    case pg2:get_local_members(Key) of
        [] ->
            {error, not_found};
        [Pid] ->
            {ok, Pid};
        {error, {no_such_group, _}} ->
            {error, not_found}
    end.
