-module(freya_snapshots_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([t_partial_snapshot/1,
         t_clear_vnode_memory/1]).

-define(th, test_helpers).


suite() ->
    [{timetrap,{seconds,30}}].

all() ->
    [
     t_partial_snapshot,
     t_clear_vnode_memory
    ].

init_per_suite(Config) ->
    ?th:setup_env(),
    application:load(freya),
    application:set_env(freya, rollup_replicas, 1),
    application:set_env(freya, rollup_read_consistency, 1),
    application:set_env(freya, rollup_write_consistency, 1),
    application:set_env(freya, rollup_dispatch_interval, 100),
    application:set_env(freya, rollup_snapshot_interval, 0),
    application:set_env(freya, rollup_edge_arrival_latency, 0),
    application:set_env(freya, rollup_vnode_arrival_latency, 0),
    ok = freya:start(),
    ?th:wait_until_node_ready(),
    Config.

end_per_suite(_Config) ->
    ok = freya:stop(),
    ok.

t_partial_snapshot(_Config) ->
    Metric = ?th:randomize(<<"t_partial_snapshot">>), Tags = [],
    Fun = sum, Precision = {15, minutes}, Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 1, Aggregate),

    ?th:keep_trying(
       ok,
       fun() ->
            {ok, Obj1}  = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
            4           = freya_object:value(Obj1),
            CassVClock  = freya_object:cass_vclock(Obj1),
            VnodeVClock = freya_object:vnode_vclock(Obj1),
            false       = vclock:equal(CassVClock, VnodeVClock),
            ok
       end,
       200, 20),

    ?th:keep_trying(
       ok,
       fun() ->
            {ok, [DP]} = freya_reader:search([{name, Metric},
                                              {tags, Tags},
                                              {start_time, Ts},
                                              {source, Aggregate}]),
            4 = freya_data_point:value(DP),
            ok
       end,
       200, 20),

    ?th:keep_trying(
       ok,
       fun() ->
            {ok, Obj1}  = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
            CassVClock  = freya_object:cass_vclock(Obj1),
            VnodeVClock = freya_object:vnode_vclock(Obj1),
            true        = vclock:equal(CassVClock, VnodeVClock),
            ok
       end,
       200, 20),
    ok.

t_clear_vnode_memory(_Config) ->
    Metric = ?th:randomize(<<"t_clear_vnode_memory">>), Tags = [],
    Fun = sum, Precision = {1, seconds}, Aggregate = {Fun, Precision},
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), Precision),
    freya_rollup:push(Metric, Tags, Ts,   1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+1, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+2, 1, Aggregate),
    freya_rollup:push(Metric, Tags, Ts+3, 1, Aggregate),

    ?th:keep_trying(
       ok,
       fun() ->
            {ok, Obj1}  = freya_get_fsm:get(Metric, Tags, Ts, Aggregate),
            4           = freya_object:value(Obj1),
            CassVClock  = freya_object:cass_vclock(Obj1),
            VnodeVClock = freya_object:vnode_vclock(Obj1),
            false       = vclock:equal(CassVClock, VnodeVClock),
            ok
       end,
       200, 20),

    ?th:keep_trying(
       ok,
       fun() ->
            {ok, [DP]} = freya_reader:search([{name, Metric},
                                              {tags, Tags},
                                              {start_time, Ts},
                                              {source, Aggregate}]),
            4 = freya_data_point:value(DP),
            ok
       end,
       200, 20),

    ?th:keep_trying(
       {error, not_found},
       fun() ->
            freya_get_fsm:get(Metric, Tags, Ts, Aggregate)
       end,
       200, 20),
    ok.
