-module(freya_stats_vnode).
-behaviour(riak_core_vnode).

-include("freya.hrl").
-include("freya_object.hrl").
-include("freya_metrics.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([push/7,
         get/6,
         repair/3,
         checkpoint/5]).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3,
         handle_tick/1]).

-record(state, {idx             :: partition(),
                node            :: node(),
                stats_table     :: ets:tid(),
                dispatch_table  :: ets:tid()
               }).

-define(TNAME(P), list_to_atom("freya_stats_"++integer_to_list(P)++"_t")).
-define(DNAME(P), list_to_atom("freya_stats_"++integer_to_list(P)++"_dispatch_t")).
-define(DEFAULT_DISPATCH_INTERVAL, timer:minutes(1)).
-define(DEFAULT_SNAPSHOT_INTERVAL, timer:minutes(1)).

%%%===================================================================
%%% API
%%%===================================================================
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

push(PrefList, Identity, Metric, Tags, Ts, Value, Aggregate) ->
    riak_core_vnode_master:command(PrefList,
                                   {push, Identity, Metric, Tags, Ts, Value, Aggregate},
                                   {fsm, undefined, self()},
                                   freya_stats_vnode_master).

get(PrefList, ReqId, Metric, Tags, Ts, Aggregate) ->
    riak_core_vnode_master:command(PrefList,
                                   {get, ReqId, Metric, Tags, Ts, Aggregate},
                                   {fsm, undefined, self()},
                                   freya_stats_vnode_master).

repair(IdxNode, Key, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Key, Obj},
                                   ignore,
                                   freya_stats_vnode_master).

checkpoint(Metric, Tags, Ts, Aggregate, VClock) ->
    Key       = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    DocIdx    = riak_core_util:chash_key(Key),
    N         = freya_rollup:replicas(),
    Preflist  = riak_core_apl:get_primary_apl(DocIdx, N, freya_stats),
    Preflist2 = [{Index, Node} || {{Index, Node}, _Type} <- Preflist],
    riak_core_vnode_master:command(Preflist2,
                                   {checkpoint, Key, VClock},
                                   ignore,
                                   freya_stats_vnode_master).

%%%===================================================================
%%% Vnode callbacks
%%%===================================================================
init([Index]) ->
    StatsTid    = ets:new(?TNAME(Index), [{keypos, #freya_object.key}]),
    DispatchTid = ets:new(?DNAME(Index), []),
    {ok, #state {idx         = Index,
                 node        = node(),
                 stats_table = StatsTid,
                 dispatch_table = DispatchTid}, [{tick, dispatch_interval()}]}.

handle_command({push, {ReqId, Coordinator}, Metric, Tags, Ts, AggrSt, Aggregate},
               _Sender, #state{stats_table=StatsTid,
                               dispatch_table=DispatchTid}=State) ->
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    Val = #val{value  = proplists:get_value(value, AggrSt),
               points = proplists:get_value(points, AggrSt)},
    case stats_find(Key, StatsTid) of
        {ok, Obj0} ->
            ok = stats_update(freya_object:update(Coordinator, Val, Obj0),
                              StatsTid, DispatchTid);
        {error, not_found} ->
            ok = stats_new(freya_object:new(Coordinator, Metric, Tags, Ts, Aggregate, Val),
                           StatsTid, DispatchTid)
    end,
    {reply, {ok, ReqId}, State};

handle_command({get, ReqId, Metric, Tags, Ts, Aggregate}, _Sender,
               #state{stats_table=StatsTid, idx=Index, node=Node}=State) ->
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    Reply = case stats_find(Key, StatsTid) of
                {ok, Obj} ->
                    Obj;
                 {error, not_found} ->
                     not_found
             end,
    {reply, {ok, ReqId, {Index, Node}, Reply}, State};

handle_command({repair, Key, Obj0}, _Sender, #state{stats_table=StatsTid,
                                                    dispatch_table=DispatchTid}=State) ->
    case stats_find(Key, StatsTid) of
        {ok, Obj1} ->
            ok = stats_update(freya_object:merge([Obj0, Obj1]),
                              StatsTid, DispatchTid);
        {error, not_found} ->
            ok = stats_new(Obj0, StatsTid, DispatchTid)
    end,
    {noreply, State};

handle_command({checkpoint, Key, VClock}, _Sender, #state{stats_table=StatsTid,
                                                          dispatch_table=DispatchTid}=State) ->
    case stats_find(Key, StatsTid) of
        {ok, Obj0} ->
            ok = stats_update(freya_object:update_cass_vclock(Obj0, VClock),
                              StatsTid, DispatchTid),
            {noreply, State};
        {error, not_found} ->
            {noreply, State}
    end.

handle_tick({deleted, State}) ->
    {cancel, State};
handle_tick(State) ->
    dispatch(State),
    {ok, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender,
                       #state{stats_table=StatsTid}=State) ->
    Acc1 = ets:foldl(fun(#freya_object{}=Obj, Acc) ->
                             Fun(Obj#freya_object.key, Obj, Acc)
                     end, Acc0, StatsTid),
    {reply, Acc1, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{stats_table=StatsTid,
                                 dispatch_table=DispatchTid}=State) ->
    {Key, Obj0} = binary_to_term(Data),
    case stats_find(Key, StatsTid) of
        {ok, Obj1} ->
            ok = stats_update(freya_object:merge([Obj0, Obj1]),
                              StatsTid, DispatchTid);
        {error, not_found} ->
            ok = stats_new(Obj0, StatsTid, DispatchTid)
    end,
    {reply, ok, State}.

encode_handoff_item(Key, Obj) ->
    term_to_binary(Key, Obj).

is_empty(#state{stats_table=StatsTid}=State) ->
    case ets:info(StatsTid, size) of
        0 -> {true, State};
        _ -> {false, State}
    end.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
stats_find(Key, StatsTid) ->
    case ets:lookup(StatsTid, Key) of
        [#freya_object{}=Obj] ->
            {ok, Obj};
        [] ->
            {error, not_found}
    end.

stats_delete(Key, StatsTid) ->
    case stats_find(Key, StatsTid) of
        {ok, _} ->
            quintana:notify_counter({?Q_VNODE_IN_MEMORY, {dec, 1}}),
            ets:delete(StatsTid, Key),
            ok;
        {error, not_found} ->
            ok
    end.

stats_new(Obj0, StatsTid, DispatchTid) ->
    quintana:notify_counter({?Q_VNODE_IN_MEMORY, {inc, 1}}),
    Obj = snapshot_new_obj(Obj0, DispatchTid),
    stats_store(Obj, StatsTid).

stats_update(Obj0, StatsTid, DispatchTid) ->
    Obj = snapshot_update_obj(Obj0, DispatchTid),
    stats_store(Obj, StatsTid).

stats_store(Obj, StatsTid) ->
    true = ets:insert(StatsTid, Obj),
    ok.

%%%===================================================================
%%% Snapshots
%%%===================================================================
% Dispatch table:
% {{update, Key}, ScheduleTime}
% {{delete, Key}, Diverged, DeleteTime}
dispatch_interval() ->
    freya:get_env(rollup_dispatch_interval, ?DEFAULT_DISPATCH_INTERVAL).

snapshot_new_obj(Obj, DispatchTid) ->
    schedule_delete(Obj, DispatchTid),
    maybe_schedule_snapshot(Obj, DispatchTid),
    Obj.

snapshot_update_obj(Obj, DispatchTid) ->
    update_delete_schedule(Obj, DispatchTid),
    maybe_schedule_snapshot(Obj, DispatchTid),
    Obj.

maybe_schedule_snapshot(#freya_object{key=Key}=Obj, DispatchTid) ->
    Diverged = freya_object:vclocks_diverged(Obj),
    case Diverged of
        true ->
            ets:insert_new(DispatchTid, {{update, Key}, next_update()});
        false ->
            ets:delete(DispatchTid, {update, Key})
    end.

next_update() ->
    tic:now_to_epoch_msecs() + freya:get_env(rollup_snapshot_interval,
                                             ?DEFAULT_SNAPSHOT_INTERVAL).

schedule_delete(#freya_object{key=Key}=Obj, DispatchTid) ->
    DeleteAt = next_delete(Obj),
    ets:insert_new(DispatchTid,
                   {{delete, Key}, freya_object:vclocks_diverged(Obj), DeleteAt}),
    Obj.

next_delete(#freya_object{ts=Ts, precision=Precision}) ->
    next_delete(Ts, Precision).

next_delete(Ts, Precision) ->
    Range        = freya_utils:ms(Precision),
    EdgeLatency  = freya_rollup:edge_arrival_latency(),
    VnodeLatency = freya_rollup:vnode_arrival_latency(),
    Ts + Range + EdgeLatency + VnodeLatency.

update_delete_schedule(#freya_object{key=Key}=Obj, DispatchTid) ->
    Diverged = freya_object:vclocks_diverged(Obj),
    ets:update_element(DispatchTid, {delete, Key}, {2, Diverged}),
    ok.

clear_delete_schedule(Key, DispatchTid) ->
    ets:delete(DispatchTid, {delete, Key}).

dispatch(State) ->
    delete_outdated(State),
    dispatch_snapshot(State).

dispatch_snapshot(#state{dispatch_table=DispatchTid, stats_table=StatsTid}) ->
    Now = tic:now_to_epoch_msecs(),
    SnapshotKeys = ets:select(DispatchTid, [{{{update,'$1'},'$2'},
                                             [{'<', '$2', Now}],
                                             ['$1']}]),
    SnapshotKeys1 = lists:filter(fun riak_governor:is_leader/1, SnapshotKeys),
    lists:foreach(dispatch_snapshot_fun(StatsTid), SnapshotKeys1),
    ok.

dispatch_snapshot_fun(StatsTid) ->
    fun(Key) ->
        case stats_find(Key, StatsTid) of
            {ok, Obj} ->
                stats_snapshot(Obj);
            {error, not_found} ->
                ok
        end
    end.

stats_snapshot(#freya_object{metric=Metric, tags=Tags, ts=Ts, fn=Fn,
                             precision=Precision}) ->
    freya_snapshot:snapshot(Metric, Tags, Ts, {Fn, Precision}).


delete_outdated(#state{dispatch_table=DispatchTid, stats_table=StatsTid}) ->
    Now = tic:now_to_epoch_msecs(),
    DeleteKeys = ets:select(DispatchTid,
                            [{{{delete,'$1'},'$2','$3'},
                              [{'andalso', {'==','$2',false}, {'<', '$3',Now}}],
                              ['$1']}]),
    lists:foreach(fun(Key) ->
                    stats_delete(Key, StatsTid),
                    clear_delete_schedule(Key, DispatchTid)
                  end, DeleteKeys),
    ok.
