-module(freya_stats_vnode).
-behaviour(riak_core_vnode).

-export([push/7]).

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

-record(state, {partition}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

push(PrefList, Identity, Metric, Tags, Ts, Value, Aggregate) ->
    riak_core_vnode_master:command(PrefList,
                                   {push, Identity, Metric, Tags, Ts, Value, Aggregate},
                                   {fsm, undefined, self()},
                                   freya_stats_vnode_master).

init([Partition]) ->
    {ok, #state { partition=Partition }}.

handle_command({push, {ReqId, _Coordinator}, _Metric, _Tags, _Ts, _Value, _Aggregate}, _Sender, State) ->
    % TODO: fill with love
    {reply, {ok, ReqId}, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
