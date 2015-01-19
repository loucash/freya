-module(freya_rollup_wrk).
-behaviour(gen_fsm).

%% API
-export([push/2]).
-export([start_link/4]).

%% states
-export([inactive/2,
         active/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {
          metric,
          tags,
          ts,
          aggregate,
          aggregator_funs,
          aggregator_st,
          timer
         }).

% how often send data to vnode
-define(EMIT_INTERVAL, 1000).

% max inactivity time after which stop this fsm
-define(SHUTDOWN_TIMEOUT, 60000).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Metric, Tags, Ts, Aggregate) ->
    gen_fsm:start_link(?MODULE, [Metric, Tags, Ts, Aggregate], []).

push(Pid, Value) ->
    gen_fsm:send_event(Pid, {push, Value}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([Metric, Tags, Ts, {Fun, Precision}]) ->
    AggregatorFuns = freya_utils:aggregator_funs(Fun),
    State = #state{metric=Metric, tags=Tags, ts=Ts,
                   aggregate={Fun, Precision},
                   aggregator_funs=AggregatorFuns},
    {ok, inactive, State, ?SHUTDOWN_TIMEOUT}.

inactive({push, Value}, State) ->
    Ref = erlang:send_after(?EMIT_INTERVAL, self(), emit),
    {next_state, active, accumulate(State#state{timer=Ref}, Value)};
inactive(timeout, State) ->
    {stop, normal, State}.

active({push, Value}, State) ->
    {next_state, active, accumulate(State, Value)};
active(emit, #state{metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate}=State) ->
    Result = freya_push_fsm:push(Metric, Tags, Ts, emit(State), Aggregate),
    case Result of
        ok ->
            {next_state, inactive, State#state{timer=undefined,
                                               aggregator_st=undefined}, ?SHUTDOWN_TIMEOUT};
        {error, _} ->
            Ref = erlang:send_after(?EMIT_INTERVAL, self(), emit),
            {next_state, active, State#state{timer=Ref}}
    end.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(Info, StateName, State) ->
    ?MODULE:StateName(Info, State).

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
accumulate(#state{aggregator_funs={Accumulate, _}, aggregator_st=AggrSt}=State, Value) ->
    State#state{aggregator_st=Accumulate(Value, AggrSt)}.

emit(#state{aggregator_funs={_, Emit}, aggregator_st=AggrSt}) ->
    Emit(AggrSt).
