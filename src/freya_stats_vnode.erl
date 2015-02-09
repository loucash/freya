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
                node        :: node(),
                stats_table :: ets:tid()}).

-define(TNAME(P), list_to_atom("freya_stats_"++integer_to_list(P)++"_t")).

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
    StatsTid = ets:new(?TNAME(Index), [{keypos, #freya_object.key}]),
    {ok, #state { idx=Index, node=node(), stats_table=StatsTid }}.

handle_command({push, {ReqId, Coordinator}, Metric, Tags, Ts, AggrSt, Aggregate},
               _Sender, #state{stats_table=StatsTid}=State) ->
    Key = freya_utils:aggregate_key(Metric, Tags, Ts, Aggregate),
    [{_, Value}, {points, Count}] = AggrSt,
    Val = #val{value=Value, points=Count},
    Obj = case stats_find(Key, StatsTid) of
              {ok, Obj0} ->
                  freya_object:update(Coordinator, Val, Obj0);
              {error, not_found} ->
                  freya_object:new(Coordinator, Metric, Tags, Ts, Aggregate, Val)
          end,
    ok = stats_store(Obj, StatsTid),
    % TODO: store to persistent store
    {reply, {ok, ReqId}, State};

handle_command({get, ReqId, Key}, _Sender,
               #state{stats_table=StatsTid, idx=Index, node=Node}=State) ->
    Reply = case stats_find(Key, StatsTid) of
                {ok, Obj} ->
                    Obj;
                {error, not_found} ->
                    not_found
            end,
    {reply, {ok, ReqId, {Index, Node}, Reply}, State};

handle_command({repair, Key, Obj0}, _Sender, #state{stats_table=StatsTid}=State) ->
    Obj = case stats_find(Key, StatsTid) of
              {ok, Obj1} ->
                  freya_object:merge([Obj0, Obj1]);
              {error, not_found} ->
                  Obj0
          end,
    ok = stats_store(Obj, StatsTid),
    {noreply, State}.

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

handle_handoff_data(Data, #state{stats_table=StatsTid}=State) ->
    {Key, Obj0} = binary_to_term(Data),
    Obj = case stats_find(Key, StatsTid) of
              {ok, Obj1} ->
                  freya_object:merge([Obj0, Obj1]);
              {error, not_found} ->
                  Obj0
          end,
    ok = stats_store(Obj, StatsTid),
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

stats_store(Obj, StatsTid) ->
    true = ets:insert(StatsTid, Obj),
    ok.
