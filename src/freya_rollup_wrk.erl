-module(freya_rollup_wrk).
-behaviour(gen_fsm).

-include("freya_metrics.hrl").

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
          aggregator_fun,
          aggregator_st = [],
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
    State = #state{metric=Metric, tags=Tags, ts=Ts,
                   aggregate={Fun, Precision},
                   aggregator_fun=aggregator(Fun)},
    quintana:notify_counter({?Q_EDGE_ROLLUP_PROCS, {inc, 1}}),
    {ok, inactive, State, ?SHUTDOWN_TIMEOUT}.

inactive({push, _}=Msg, State) ->
    Ref = erlang:send_after(?EMIT_INTERVAL, self(), emit),
    {next_state, active, accumulate(Msg, State#state{timer=Ref})};
inactive(timeout, State) ->
    {stop, normal, State}.

active({push, _}=Msg, State) ->
    {next_state, active, accumulate(Msg, State)};
active(emit, #state{metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate}=State) ->
    Result = freya_push_fsm:push(Metric, Tags, Ts, emit(State), Aggregate),
    case Result of
        ok ->
            {next_state, inactive, State#state{timer=undefined,
                                               aggregator_st=[]}, ?SHUTDOWN_TIMEOUT};
        {error, _} ->
            Ref = erlang:send_after(?EMIT_INTERVAL, self(), emit),
            {next_state, active, State#state{timer=Ref}}
    end.

handle_event(_Event, _StateName, State) ->
    {stop, badarg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badarg, State}.

handle_info(Info, StateName, State) ->
    ?MODULE:StateName(Info, State).

terminate(_Reason, _StateName, _State) ->
    quintana:notify_counter({?Q_EDGE_ROLLUP_PROCS, {dec, 1}}),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
accumulate(Msg, #state{aggregator_st=AggrSt, aggregator_fun=AggrFun}=State) ->
    State#state{aggregator_st=AggrFun(Msg, AggrSt)}.

emit(#state{aggregator_st=AggrSt, aggregator_fun=AggrFun}) ->
    AggrFun(emit, AggrSt).

aggregator(Fun) ->
    {Accumulate, Emit} = freya_utils:aggregator_funs(Fun),
    {Count, EmitCount} = freya_utils:aggregator_funs(count),
    Get                = fun proplists:get_value/2,
    fun({push, Value}, AggrSt) when is_list(AggrSt) ->
            [{value,    Accumulate(Value, Get(value,    AggrSt))},
             {points,   Count(            Get(points, AggrSt))}];
       (emit, []) -> [];
       (emit, AggrSt) ->
            [{value,    Emit(       Get(value,    AggrSt))},
             {points,   EmitCount(  Get(points, AggrSt))}]
    end.
