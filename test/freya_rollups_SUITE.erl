-module(freya_rollups_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, suite/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([t_edge_sum/1, t_edge_max/1, t_edge_min/1, t_edge_avg/1]).
-export([t_late_metrics/1]).
-export([t_vnode_sum/1, t_vnode_max/1, t_vnode_min/1, t_vnode_avg/1]).

-define(th, test_helpers).


suite() ->
    [{timetrap,{seconds,30}}].

groups() ->
    [
     {edge, [],
      [
       t_edge_sum,
       t_edge_max,
       t_edge_min,
       t_edge_avg
      ]},
     {delay, [],
      [
       t_late_metrics
      ]},
     {vnode, [],
      [
       t_vnode_sum,
       t_vnode_max,
       t_vnode_min,
       t_vnode_avg
      ]}
    ].

all() ->
    [
     {group, edge},
     {group, delay},
     {group, vnode}
    ].

init_per_suite(Config) ->
    ?th:setup_env(),
    application:load(freya),
    application:set_env(freya, n, 1),
    application:set_env(freya, r, 1),
    application:set_env(freya, w, 1),
    ok = freya:start(),
    ?th:wait_until_node_ready(),
    Config.

end_per_suite(_Config) ->
    ok = freya:stop(),
    ok.

t_edge_sum(_Config) ->
    meck_vnode_push(),
    Metric = <<"t_edge_sum">>, Tags = [], Precision = {15, minutes}, Fun = sum,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 1, Aggregate),
    AggrSt = [{value, 4}, {points, 4}],
    ?th:keep_trying_receive({push, Metric, Tags, Ts, AggrSt, Aggregate}, 200, 10),
    unload_vnode_push(),
    ok.

t_edge_max(_Config) ->
    meck_vnode_push(),
    Metric = <<"t_edge_max">>, Tags = [], Precision = {15, minutes}, Fun = max,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 2, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 3, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 4, Aggregate),
    AggrSt = [{value, 4}, {points, 4}],
    ?th:keep_trying_receive({push, Metric, Tags, Ts, AggrSt, Aggregate}, 200, 10),
    unload_vnode_push(),
    ok.

t_edge_min(_Config) ->
    meck_vnode_push(),
    Metric = <<"t_edge_min">>, Tags = [], Precision = {15, minutes}, Fun = min,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 2, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 3, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 4, Aggregate),
    AggrSt = [{value, 1}, {points, 4}],
    ?th:keep_trying_receive({push, Metric, Tags, Ts, AggrSt, Aggregate}, 200, 10),
    unload_vnode_push(),
    ok.

t_edge_avg(_Config) ->
    meck_vnode_push(),
    Metric = <<"t_edge_avg">>, Tags = [], Precision = {15, minutes}, Fun = avg,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 2, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 3, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 4, Aggregate),
    AggrSt = [{value, 2.5}, {points, 4}],
    ?th:keep_trying_receive({push, Metric, Tags, Ts, AggrSt, Aggregate}, 200, 10),
    unload_vnode_push(),
    ok.

t_late_metrics(_Config) ->
    MaxDelay = 1,
    Metric = <<"t_late_metrics">>, Tags = [], Precision = {1, seconds}, Fun = sum,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    ok = freya_rollup:push(Metric, Tags, Ts, 1, Aggregate, MaxDelay),
    ?th:keep_trying(true, fun() -> tic:now_to_epoch_msecs() > Ts+2000 end,
                    100, 21),
    {error, too_late} = freya_rollup:push(Metric, Tags, Ts+1, 1, Aggregate, MaxDelay),
    ok.

t_vnode_sum(_Config) ->
    Metric = <<"t_vnode_sum">>, Tags = [], Precision = {15, minutes}, Fun = sum,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 1, Aggregate),
    ?th:keep_trying(4,
                    fun() ->
                        {ok, Obj} = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
                        freya_object:value(Obj)
                    end,
                    200, 10),
    ok.

t_vnode_max(_Config) ->
    Metric = <<"t_vnode_max">>, Tags = [], Precision = {15, minutes}, Fun = max,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 2, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 3, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 4, Aggregate),
    ?th:keep_trying(4,
                    fun() ->
                        {ok, Obj} = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
                        freya_object:value(Obj)
                    end,
                    200, 10),
    ok.

t_vnode_min(_Config) ->
    Metric = <<"t_vnode_min">>, Tags = [], Precision = {15, minutes}, Fun = min,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 2, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 3, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 4, Aggregate),
    ?th:keep_trying(1,
                    fun() ->
                        {ok, Obj} = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
                        freya_object:value(Obj)
                    end,
                    200, 10),
    ok.

t_vnode_avg(_Config) ->
    Metric = <<"t_vnode_avg">>, Tags = [], Precision = {15, minutes}, Fun = avg,
    Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 2, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 3, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 4, Aggregate),
    ?th:keep_trying(2.5,
                    fun() ->
                        {ok, Obj} = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
                        freya_object:value(Obj)
                    end,
                    200, 10),
    ok.

%%%===================================================================
%%% Internal
%%%===================================================================
meck_vnode_push() ->
    TestPid = self(),
    meck:new(freya_stats_vnode, [passthrough]),
    meck:expect(freya_stats_vnode, push,
                fun(Preflist, Identity, Metric, Tags, Ts, Value, Aggregate) ->
                    TestPid ! {push, Metric, Tags, Ts, Value, Aggregate},
                    meck:passthrough([Preflist, Identity, Metric, Tags, Ts, Value, Aggregate])
                end).

unload_vnode_push() ->
    meck:unload(freya_stats_vnode).
