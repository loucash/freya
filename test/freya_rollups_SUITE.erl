-module(freya_rollups_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, suite/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([t_edge_sum/1, t_edge_max/1, t_edge_min/1, t_edge_avg/1]).

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
      ]}
    ].

all() ->
    [
     {group, edge}
    ].

init_per_suite(Config) ->
    ?th:setup_env(),
    ok = freya:start(),
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
    ?th:keep_trying_receive({push, Metric, Tags, Ts, 4, Aggregate}, 200, 10),
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
    ?th:keep_trying_receive({push, Metric, Tags, Ts, 4, Aggregate}, 200, 10),
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
    ?th:keep_trying_receive({push, Metric, Tags, Ts, 1, Aggregate}, 200, 10),
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
    ?th:keep_trying_receive({push, Metric, Tags, Ts, {2.5, 4}, Aggregate}, 200, 10),
    unload_vnode_push(),
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
