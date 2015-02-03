-module(freya_stats_vnode).
-behaviour(riak_core_vnode).

-include("freya_object.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([push/7,
         get/3,
         repair/3]).

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
         handle_exit/3]).

-record(state, {idx,
                node,
                stats}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

push(PrefList, Identity, Metric, Tags, Ts, Value, Aggregate) ->
    riak_core_vnode_master:command(PrefList,
                                   {push, Identity, Metric, Tags, Ts, Value, Aggregate},
                                   {fsm, undefined, self()},
                                   freya_stats_vnode_master).

get(PrefList, ReqId, Key) ->
    riak_core_vnode_master:command(PrefList,
                                   {get, ReqId, Key},
                                   {fsm, undefined, self()},
                                   freya_stats_vnode_master).

repair(IdxNode, Key, Obj) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair, Key, Obj},
                                   ignore,
                                   freya_stats_vnode_master).

init([Index]) ->
    {ok, #state { idx=Index, node=node(), stats=dict:new() }}.

handle_command({push, {ReqId, Coordinator}, Metric, Tags, Ts, AggrSt, Aggregate},
               _Sender, #state{stats=Stats0}=State) ->
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    [{_, Value}, {points, Count}] = AggrSt,
    Val = #val{value=Value, points=Count},
    Obj = case dict:find(Key, Stats0) of
              {ok, Obj0} ->
                  freya_object:update(Coordinator, Val, Obj0);
              error ->
                  freya_object:new(Coordinator, Metric, Tags, Ts, Aggregate, Val)
          end,
    Stats = dict:store(Key, Obj, Stats0),
    % TODO: store to persistent store
    {reply, {ok, ReqId}, State#state{stats=Stats}};

handle_command({get, ReqId, Key}, _Sender,
               #state{stats=Stats, idx=Index, node=Node}=State) ->
    Reply = case dict:find(Key, Stats) of
                error ->
                    not_found;
                {ok, Obj} ->
                    Obj
            end,
    {reply, {ok, ReqId, {Index, Node}, Reply}, State};

handle_command({repair, Key, Obj}, _Sender, #state{stats=Stats0}=State) ->
    Stats = dict:store(Key, Obj, Stats0),
    {noreply, State#state{stats=Stats}}.


handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, _Sender, State) ->
    Acc = dict:fold(Fun, Acc0, State#state.stats),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, #state{stats=Stats0}=State) ->
    {StatName, Obj1} = binary_to_term(Data),
    Obj =
        case dict:find(StatName, Stats0) of
            {ok, Obj2} -> freya_object:merge([Obj1,Obj2]);
            error -> Obj1
        end,
    Stats = dict:store(StatName, Obj, Stats0),
    {reply, ok, State#state{stats=Stats}}.

encode_handoff_item(Key, Obj) ->
    term_to_binary(Key, Obj).

is_empty(State) ->
    case dict:size(State#state.stats) of
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
