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
    ?FORALL({MetricName, Timestamp, DataType, Tags, Value},
            {metric_name(), timestamp(), data_type(), tags(), value()},
           begin
               DataPoint1 = #data_point{name=MetricName, ts=Timestamp,
                                        type=DataType, tags=Tags, value=Value},
               {ok, {Row, Ts, Val}} = freya_blobs:encode(DataPoint1),
               {ok, DataPoint2} = freya_blobs:decode(Row, Ts, Val),
               DataPoint1 =:= DataPoint2#data_point{row_time=undefined}
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

data_type() ->
    oneof([<<"kairos_long">>]).

tags() ->
    list(?LET({K,V}, {utf8_bin(), utf8_bin()}, {K,V})).
