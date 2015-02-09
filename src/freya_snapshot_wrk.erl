-module(freya_snapshot_wrk).
-behaviour(gen_fsm).

-include("freya.hrl").

%% API
-export([start_link/4]).

%% states
-export([execute/2,
         waiting/2,
         checkpoint/2]).

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
          ttl,
          req_id,
          obj
         }).

-define(DEFAULT_TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Metric, Tags, Ts, Aggregate) ->
    gen_fsm:start_link(?MODULE, [Metric, Tags, Ts, Aggregate], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([Metric, Tags, Ts, {Fun, Precision}]) ->
    TTL = infinity, % TODO: pass ttl from the edge or take it from config
    State = #state{metric=Metric, tags=Tags, ts=Ts,
                   aggregate={Fun, Precision}, ttl=TTL},
    {ok, execute, State, 0}.

execute(timeout, #state{metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate}=State) ->
    {ok, ReqId} = freya_get_fsm:get_async(Metric, Tags, Ts, Aggregate),
    {next_state, waiting, State#state{req_id=ReqId}, ?DEFAULT_TIMEOUT}.

waiting({ok, ReqId, Obj}, #state{req_id=ReqId, metric=Metric, tags=Tags,
                                 ts=Ts, aggregate=Aggregate, ttl=TTL}=State0) ->
    case freya_object:vclocks_diverged(Obj) of
        true ->
            DP = freya_data_point:new(Metric, Ts, freya_object:value(Obj), Tags),
            State = State0#state{obj=Obj},
            case freya_writer:save_data_point(DP, [{ttl, TTL}, {aggregate, Aggregate}]) of
                ok ->
                    {next_state, checkpoint, State, 0};
                {error, _} ->
                    {stop, normal, State}
            end;
        false ->
            {stop, normal, State0}
    end;
waiting(timeout, State) ->
    {stop, normal, State}.

checkpoint(timeout, #state{metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate,
                           obj=Obj}=State) ->
    VClock = freya_object:vnode_vclock(Obj),
    freya_stats_vnode:checkpoint(Metric, Tags, Ts, Aggregate, VClock),
    {stop, normal, State}.

handle_event(_Event, _StateName, State) ->
    {stop, badarg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badarg, State}.

handle_info(Info, StateName, State) ->
    ?MODULE:StateName(Info, State).

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
