-module(freya_push_fsm).

-behaviour(gen_fsm).

%% API
-export([push/5]).
-export([start_link/7]).

-export([prepare/2,
         execute/2,
         waiting/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {req_id          :: reference(),
                coordinator     :: node(),
                from            :: pid(),
                preflist        :: riak_core_apl:preflist2(),
                responses = 0   :: non_neg_integer(),

                metric,
                tags,
                ts,
                value,
                aggregate,

                n               :: non_neg_integer(),
                w               :: non_neg_integer()}).

-define(TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

push(Metric, Tags, Ts, Value, Aggregate) ->
    ReqId = make_ref(),
    freya_push_fsm_sup:start_child([ReqId, self(), Metric, Tags, Ts, Value,
                                    Aggregate]),
    freya_utils:wait_for_reqid(ReqId, ?TIMEOUT).

start_link(ReqId, From, Metric, Tags, Ts, Value, Aggregate) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Metric, Tags, Ts, Value, Aggregate], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([ReqId, From, Metric, Tags, Ts, Value, Aggregate]) ->
    {ok, N} = freya:get_env(n),
    {ok, W} = freya:get_env(w),
    State = #state{req_id=ReqId, coordinator=node(), from=From, n=N, w=W,
                   metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate,
                   value=Value},
    {ok, prepare, State, 0}.

prepare(timeout, #state{metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate, n=N}=State) ->
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, N, freya_stats),
    {next_state, execute, State#state{preflist=Preflist}, 0}.

execute(timeout, #state{preflist=PrefList, req_id=ReqId, coordinator=Coordinator,
                        metric=Metric, tags=Tags, ts=Ts, value=Value,
                        aggregate=Aggregate}=State) ->
    freya_stats_vnode:push(PrefList, {ReqId, Coordinator},
                           Metric, Tags, Ts, Value, Aggregate),
    {next_state, waiting, State}.

waiting({ok, ReqId}, #state{req_id=ReqId, from=From, responses=Resp0, w=W}=State0) ->
    Resp = Resp0 + 1,
    State = State0#state{responses=Resp},
    case Resp =:= W of
        true ->
            From ! {ok, ReqId},
            {stop, normal, State};
        false ->
            {next_state, waiting, State}
    end.

handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, badmsg, State}.

handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
