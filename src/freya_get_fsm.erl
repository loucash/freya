-module(freya_get_fsm).
-behaviour(gen_fsm).

%% API
-export([get/4]).
-export([start_link/6]).

-export([prepare/2,
         execute/2,
         waiting/2,
         wait_for_n/2,
         finalize/2]).

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
                replies = []    :: list(),

                metric,
                tags,
                ts,
                aggregate,
                key,

                n               :: non_neg_integer(),
                r               :: non_neg_integer()}).

-define(TIMEOUT, 5000).
-define(WAIT_TIMEOUT, 10000).

%%%===================================================================
%%% API
%%%===================================================================

get(Metric, Tags, Ts, Aggregate) ->
    ReqId = make_ref(),
    freya_get_fsm_sup:start_child([ReqId, self(), Metric, Tags, Ts, Aggregate]),
    freya_utils:wait_for_reqid(ReqId, ?TIMEOUT).

start_link(ReqId, From, Metric, Tags, Ts, Aggregate) ->
    gen_fsm:start_link(?MODULE, [ReqId, From, Metric, Tags, Ts, Aggregate], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([ReqId, From, Metric, Tags, Ts, Aggregate]) ->
    {ok, N} = freya:get_env(n),
    {ok, R} = freya:get_env(r),
    State = #state{req_id=ReqId, coordinator=node(), from=From, n=N, r=R,
                   metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate},
    {ok, prepare, State, 0}.

prepare(timeout, #state{metric=Metric, tags=Tags, ts=Ts, aggregate=Aggregate, n=N}=State) ->
    Key       = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    DocIdx    = riak_core_util:chash_key(Key),
    Preflist  = riak_core_apl:get_primary_apl(DocIdx, N, freya_stats),
    Preflist2 = [{Index, Node} || {{Index, Node}, _Type} <- Preflist],
    {next_state, execute, State#state{preflist=Preflist2, key=Key}, 0}.

execute(timeout, #state{preflist=PrefList, req_id=ReqId, key=Key}=State) ->
    freya_stats_vnode:get(PrefList, ReqId, Key),
    {next_state, waiting, State}.

waiting({ok, ReqId, IdxNode, Obj},
        #state{req_id=ReqId, from=From, responses=Resp0, replies=Replies0,
               r=R, n=N}=State0) ->
    Resp    = Resp0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    State   = State0#state{responses=Resp, replies=Replies},
    case Resp =:= R of
        true ->
            Reply = freya_object:value(merge(Replies)),
            From ! {ok, ReqId, Reply},

            case Resp =:= N of
                true ->
                    {next_state, finalize, State, 0};
                false ->
                    {next_state, wait_for_n, State, ?WAIT_TIMEOUT}
            end;
        false ->
            {next_state, waiting, State}
    end.

wait_for_n({ok, _ReqId, IdxNode, Obj}, #state{responses=Resp0, replies=Replies0,
                                              n=N}=State0) ->
    Resp    = Resp0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    State   = State0#state{responses=Resp, replies=Replies},
    case Resp =:= N of
        true ->
            {next_state, finalize, State, 0};
        false ->
            {next_state, wait_for_n, State, ?WAIT_TIMEOUT}
    end;

wait_for_n(timeout, State) ->
    % todo: partial repair
    {stop, normal, State}.

finalize(timeout, #state{replies=Replies, key=Key}=State) ->
    MObj = merge(Replies),
    case needs_repair(MObj, Replies) of
        true ->
            repair(Key, MObj, Replies),
            {stop, notmal, State};
        false ->
            {stop, normal, State}
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

%%%===================================================================
%%% Internal functions
%%%===================================================================
merge(Replies) ->
    freya_object:merge([Obj || {_, Obj} <- Replies]).

needs_repair(MObj, Replies) ->
    freya_object:needs_repair(MObj, [Obj || {_, Obj} <- Replies]).

repair(_Key, _MObj, []) -> ok;
repair(Key, MObj, [{IdxNode, Obj}|Replies]) ->
    case freya_object:equal(MObj, Obj) of
        true ->
            repair(Key, MObj, Replies);
        false ->
            freya_stats_vnode:repair(IdxNode, Key, MObj),
            repair(Key, MObj, Replies)
    end.
