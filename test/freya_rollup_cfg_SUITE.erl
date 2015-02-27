-module(freya_rollup_cfg_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([t_single/1,
         t_wildcard_metric/1,
         t_wildcard_tag_name/1,
         t_wildcard_tag_value/1,
         t_multiple_tags/1,
         t_no_tags_in_spec/1,
         t_no_matches/1,
         t_multiple_matches/1,
         t_no_duplicated_aggregates/1,
         t_match_with_ns/1]).

-define(th, test_helpers).


suite() ->
    [{timetrap,{seconds,30}}].

all() ->
    [
     t_single,
     t_wildcard_metric,
     t_wildcard_tag_name,
     t_wildcard_tag_value,
     t_multiple_tags,
     t_no_matches,
     t_multiple_matches,
     t_no_tags_in_spec,
     t_no_duplicated_aggregates,
     t_match_with_ns
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

t_single(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{<<"t1">>, <<"v1">>}], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_wildcard_metric(_Config) ->
    Config =
    [{default,
      [
       {'_', [{<<"t1">>, <<"v1">>}], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_wildcard_tag_name(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{'_', <<"v1">>}], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_wildcard_tag_value(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{<<"t1">>, '_'}], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_multiple_tags(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{<<"t1">>, <<"v1">>},
                    {<<"t2">>, <<"v2">>}], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    Expected = [{{avg, {15, minutes}}, [{ttl, 123}]}],
    Result = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>,
                                       [{<<"t1">>, <<"v1">>},
                                        {<<"t2">>, <<"v2">>}]),
    Expected = Result,
    ok.

t_no_tags_in_spec(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_no_matches(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{<<"t1">>, <<"v1">>}], {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [] = freya_rollup_cfg:do_match(Dispatch, default, <<"mem">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_multiple_matches(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{<<"t1">>, <<"v1">>}], {avg, {15, minutes}}, [{ttl, 123}]},
       {'_', [{<<"t1">>, <<"v1">>}], {sum, {1, hour}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]},
     {{sum, {1, hour}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_no_duplicated_aggregates(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>, [{<<"t1">>, <<"v1">>}], {avg, {15, minutes}}, [{ttl, 123}]},
       {'_', '_', {avg, {15, minutes}}, [{ttl, 123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {15, minutes}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, <<"cpu">>, [{<<"t1">>, <<"v1">>}]),
    ok.

t_match_with_ns(_Config) ->
    Config =
    [{default,
      [
       {<<"cpu">>,'_', {avg,{1,seconds}}, [{ttl,123}]}
      ]}],
    Dispatch = freya_rollup_cfg:compile(Config),
    [{{avg, {1, seconds}}, [{ttl, 123}]}] = freya_rollup_cfg:do_match(Dispatch, default, {<<"ns">>, <<"cpu">>}, []),
    ok.
