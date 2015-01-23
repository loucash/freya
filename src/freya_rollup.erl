-module(freya_rollup).

-behaviour(gen_server).

%% API
-export([push/5]).
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

push(Metric, Tags, Ts, Value, {_Fun, Precision}=Aggregate) ->
    AlignedTs       = freya_utils:floor(Ts, Precision),
    SanitizedTags   = freya_utils:sanitize_tags(Tags),
    Key             = freya_utils:aggregate_key(Metric, SanitizedTags, AlignedTs, Aggregate),
    {ok, Pid} = case find_worker_process(Key) of
                    {ok, _} = Ok ->
                        Ok;
                    {error, not_found} ->
                        create(Metric, SanitizedTags, AlignedTs, Aggregate)
                end,
    freya_rollup_wrk:push(Pid, Value).

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
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
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
find_worker_process(Key) ->
    case pg2:get_local_members(Key) of
        [] ->
            {error, not_found};
        [Pid] ->
            {ok, Pid};
        {error, {no_such_group, _}} ->
            {error, not_found}
    end.
