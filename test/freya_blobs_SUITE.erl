-module(freya_blobs_SUITE).
-author('Łukasz Biedrycki <lukasz.biedrycki@gmail.com>').

-export([all/0, suite/0]).
-export([t_encode_decode_rowkey/1]).

-include("freya.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("proper/include/proper.hrl").

-define(NUMTESTS, 1000).
-define(PROPTEST(A), true = proper:quickcheck(A(),
                                              [{numtests, ?NUMTESTS},
                                               {constraint_tries, 1000}])).


suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
        t_encode_decode_rowkey
    ].

%% Test codec symmetrically
t_encode_decode_rowkey(_Config) ->
    ?PROPTEST(prop_encode_decode_rowkey).

%% PropEr
prop_encode_decode_rowkey() ->
    ?FORALL({MetricName, Timestamp, Tags, Value},
            {metric_name(), timestamp(), tags(), value()},
           begin
               DP1 = freya_data_point:new(MetricName, Timestamp, Value, Tags),
               {ok, {Row, Ts, Val}} = freya_data_point:encode(DP1),
               {ok, DP2} = freya_data_point:decode(Row, Ts, Val),
               DP1 =:= DP2
           end).

metric_name() ->
    non_empty(utf8_bin()).

utf8_bin() ->
    ?LET(S,
         list(oneof([integer(16#30, 16#39),
                     integer(16#41, 16#5A),
                     integer(16#61, 16#7A)])),
         list_to_binary(S)).

timestamp() ->
    proper_types:non_neg_integer().

value() ->
    proper_types:non_neg_integer().

tags() ->
    list(?LET({K,V}, {utf8_bin(), utf8_bin()}, {K,V})).
