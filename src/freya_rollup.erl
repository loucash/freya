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

-record(state, {
          dispatch_table    :: ets:id(),
          monitor = []      :: [{pid(), reference()}]
         }).

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
    DTid = ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    {ok, #state{dispatch_table=DTid}}.

handle_call({create, Metric, Tags, Ts, Aggregate}, _From,
            #state{monitor=Monitors}=State) ->
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    case find_worker_process(Key) of
        {ok, _} = Ok ->
            {reply, Ok, State};
        {error, not_found} ->
            {ok, Pid} = freya_rollup_sup:start_child([Metric, Tags, Ts, Aggregate]),
            ets:insert(?MODULE, {Key, Pid}),
            MRef = monitor(process, Pid),
            {reply, {ok, Pid}, State#state{monitor=[{Pid, MRef}|Monitors]}}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, _, _, _}, #state{monitor=Monitors}=State) ->
    case lists:keyfind(MRef,2,Monitors) of
        false ->
            {noreply, State};
        {Pid, _} = Found ->
            true = ets:match_delete(?MODULE, {'_', Pid}),
            {noreply, State#state{monitor = Monitors -- [Found]}}
    end;
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
    case ets:lookup(?MODULE, Key) of
        [{_, Pid}] ->
            {ok, Pid};
        [] ->
            {error, not_found}
    end.
