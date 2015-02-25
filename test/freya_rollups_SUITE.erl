-module(freya_rollups_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, suite/0, groups/0, init_per_suite/1, end_per_suite/1]).
-export([t_late_metrics/1]).
-export([t_vnode_sum/1, t_vnode_max/1, t_vnode_min/1, t_vnode_avg/1]).
-export([t_end2end_simple/1]).

-define(th, test_helpers).


suite() ->
    [{timetrap,{seconds,30}}].

groups() ->
    [
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
      ]},
     {end2end, [],
      [
       t_end2end_simple
      ]}
    ].

all() ->
    [
     {group, delay},
     {group, vnode},
     {group, end2end}
    ].

init_per_suite(Config) ->
    ?th:setup_env(),
    application:load(freya),
    application:set_env(freya, rollup_replicas, 1),
    application:set_env(freya, rollup_read_consistency, 1),
    application:set_env(freya, rollup_write_consistency, 1),
    application:set_env(freya, rollup_dispatch_interval, 500),
    application:set_env(freya, rollup_snapshot_interval, 0),
    application:set_env(freya, rollup_edge_arrival_latency, 0),
    application:set_env(freya, rollup_vnode_arrival_latency, 0),
    MetricName = ?th:randomize(<<"raw_to_rollup">>),
    application:set_env(
      freya, rollup_config,
      [{default, [{MetricName, '_', {avg, {1, seconds}}, [{ttl, 123}]},
                  {MetricName, '_', {sum, {1, seconds}}, [{ttl, 123}]},
                  {MetricName, '_', {max, {1, seconds}}, [{ttl, 123}]},
                  {MetricName, '_', {min, {1, seconds}}, [{ttl, 123}]}
                 ]}]),
    freya:start(),
    frik:start(),
    ?th:wait_until_node_ready(),
    [{raw_to_rollup_metric, MetricName}|Config].

end_per_suite(_Config) ->
    ok = freya:stop(),
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

t_end2end_simple(Config) ->
    Ns = <<"t_e2e_simple">>,
    Metric = ?config(raw_to_rollup_metric, Config),
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), {1, seconds}),
    ok = frik:put_metrics(Ns, [{Metric, [[Ts,   1],
                                         [Ts+1, 2],
                                         [Ts+2, 3],
                                         [Ts+3, 4]]}]),
    ?th:keep_trying(2.5,
                    fun() ->
                            {ok, [DP]} = freya_reader:search(
                                           [{ns, Ns},
                                            {name, Metric},
                                            {start_time, Ts},
                                            {source, {avg, {1, seconds}}}]),
                            freya_data_point:value(DP)
                    end,
                    2000, 10),
    ?th:keep_trying(10,
                    fun() ->
                            {ok, [DP]} = freya_reader:search(
                                           [{ns, Ns},
                                            {name, Metric},
                                            {start_time, Ts},
                                            {source, {sum, {1, seconds}}}]),
                            freya_data_point:value(DP)
                    end,
                    2000, 10),
    ?th:keep_trying(1,
                    fun() ->
                            {ok, [DP]} = freya_reader:search(
                                           [{ns, Ns},
                                            {name, Metric},
                                            {start_time, Ts},
                                            {source, {min, {1, seconds}}}]),
                            freya_data_point:value(DP)
                    end,
                    2000, 10),
    ?th:keep_trying(4,
                    fun() ->
                            {ok, [DP]} = freya_reader:search(
                                           [{ns, Ns},
                                            {name, Metric},
                                            {start_time, Ts},
                                            {source, {max, {1, seconds}}}]),
                            freya_data_point:value(DP)
                    end,
                    2000, 10),
    ok.
