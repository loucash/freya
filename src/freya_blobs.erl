%%%-------------------------------------------------------------------
%%% @doc
%%% Module with codecs for rowkey
%%% @end
%%%-------------------------------------------------------------------
-module(freya_blobs).

-export([encode_rowkey/4,
         encode_rowkey/5,
         encode_timestamp/1,
         encode_timestamp/2,
         encode_offset/1,
         encode_value/2]).
-export([decode_rowkey/1,
         decode_timestamp/2,
         decode_offset/1,
         decode_value/2]).
-export([encode_search_key/3]).

-include("freya.hrl").

-define(MODEL_VERSION, 16#01).

-define(TYPE_LONG,   16#01).
-define(TYPE_DOUBLE, 16#02).

-define(RAW_DATA, 16#00).

-define(FUN_MAX, 16#01).
-define(FUN_MIN, 16#02).
-define(FUN_AVG, 16#03).
-define(FUN_SUM, 16#04).

%%%===================================================================
%%% API
%%%===================================================================
-spec encode_rowkey(metric_name(), milliseconds(), data_type(), data_tags()) ->
    {ok, binary()}.
encode_rowkey(MetricName, Ts, Type, Tags) ->
    encode_rowkey(MetricName, Ts, Type, Tags, raw).

encode_rowkey(MetricName, Ts, Type, Tags0, DataPrecision) ->
    RowWidth         = freya_utils:row_width(DataPrecision),
    RowTime          = freya_utils:floor(Ts, RowWidth),
    MetricNameLength = byte_size(MetricName),
    {AggregateFun,
     AggregateParam1,
     AggregateParam2} = encode_data_precision(DataPrecision),
    DataType         = encode_data_type(Type),
    Tags             = encode_tags(Tags0),
    TagsLength       = byte_size(Tags),

    Bin = <<?MODEL_VERSION:8/integer,
            MetricNameLength:16/integer,
            MetricName/binary,
            AggregateFun:8/integer,
            AggregateParam1:64/integer,
            AggregateParam2:64/integer,
            RowTime:64/integer,
            DataType:8/integer,
            TagsLength:16/integer,
            Tags/binary>>,
    {ok, Bin}.

-spec decode_rowkey(binary()) -> {ok, proplists:proplist()} | {error, any()}.
decode_rowkey(<<?MODEL_VERSION:8/integer,
                MetricNameLength:16/integer,
                MetricName:MetricNameLength/binary-unit:8,
                AggregateFun:8/integer,
                AggregateParam1:64/integer,
                AggregateParam2:64/integer,
                RowTime:64/integer,
                DataType:8/integer,
                TagsLength:16/integer,
                Tags:TagsLength/binary-unit:8
              >>) ->
    Result = [{name,        MetricName},
              {row_time,    RowTime},
              {type,        decode_data_type(DataType)},
              {tags,        decode_tags(Tags)},
              {precision,   decode_data_precision(AggregateFun,
                                                  AggregateParam1,
                                                  AggregateParam2)}],
    {ok, Result}.

-spec encode_timestamp(milliseconds()) -> {ok, binary()}.
encode_timestamp(Ts) ->
    encode_timestamp(Ts, raw).

-spec encode_timestamp(milliseconds(), data_precision()) -> {ok, binary()}.
encode_timestamp(Ts, DataPrecision) ->
    RowWidth = freya_utils:row_width(DataPrecision),
    RowTime  = freya_utils:floor(Ts, RowWidth),
    Offset   = Ts - RowTime,
    encode_offset(Offset).

-spec encode_offset(milliseconds()) -> {ok, binary()}.
encode_offset(Offset) ->
    {ok, <<Offset:64/integer>>}.

-spec decode_timestamp(binary(), milliseconds()) -> {ok, milliseconds()}.
decode_timestamp(OffsetBin, RowTime) ->
    {ok, Offset} = decode_offset(OffsetBin),
    {ok, RowTime+Offset}.

-spec decode_offset(binary()) -> {ok, milliseconds()}.
decode_offset(<<Offset:64/integer>>) ->
    {ok, Offset}.

-spec encode_value(binary(), any()) -> {ok, binary()}.
encode_value(long, Value) ->
    {ok, pack_long(Value)};
encode_value(double, Value) ->
    {ok, <<Value/float>>}.

-spec decode_value(any(), binary()) -> {ok, any()}.
decode_value(Value, long) ->
    {ok, unpack_long(Value)};
decode_value(<<Value/float>>, double) ->
    {ok, Value}.

-spec encode_search_key(metric_name(), milliseconds(), data_precision()) -> {ok, binary()}.
encode_search_key(MetricName, Ts, DataPrecision) ->
    MetricNameLength = byte_size(MetricName),
    {AggregateFun,
     AggregateParam1,
     AggregateParam2} = encode_data_precision(DataPrecision),

    Bin = <<?MODEL_VERSION:8/integer,
            MetricNameLength:16/integer,
            MetricName/binary,
            AggregateFun:8/integer,
            AggregateParam1:64/integer,
            AggregateParam2:64/integer,
            Ts:64/integer>>,
    {ok, Bin}.

%%%===================================================================
%%% Internal
%%%===================================================================
encode_data_precision(raw) ->
    {?RAW_DATA, ?RAW_DATA, ?RAW_DATA};
encode_data_precision({Fun, Precision}) ->
    {encode_aggregate_fun(Fun), freya_utils:ms(Precision), ?RAW_DATA}.

decode_data_precision(?RAW_DATA, _, _) -> raw;
decode_data_precision(Fun, Precision, _) ->
    {decode_aggregate_fun(Fun), Precision}.

encode_aggregate_fun(max) -> ?FUN_MAX;
encode_aggregate_fun(min) -> ?FUN_MIN;
encode_aggregate_fun(avg) -> ?FUN_AVG;
encode_aggregate_fun(sum) -> ?FUN_SUM.

decode_aggregate_fun(?FUN_MAX) -> max;
decode_aggregate_fun(?FUN_MIN) -> min;
decode_aggregate_fun(?FUN_AVG) -> avg;
decode_aggregate_fun(?FUN_SUM) -> sum.

encode_data_type(long)   -> ?TYPE_LONG;
encode_data_type(double) -> ?TYPE_DOUBLE.

decode_data_type(?TYPE_LONG) -> long;
decode_data_type(?TYPE_DOUBLE) -> double.

encode_tags([]) -> <<>>;
encode_tags(Tags) ->
    msgpack:pack(freya_utils:sanitize_tags(Tags), [{format,jsx}]).

decode_tags(<<>>) -> [];
decode_tags(Tags) -> msgpack:unpack(Tags, [{format,jsx}]).

% https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
pack_long(Value) when is_integer(Value) ->
    pack_unsigned_long((Value bsl 1) bxor  (Value bsr 63)).

unpack_long(ValueBin) when is_binary(ValueBin) ->
    Value = unpack_unsigned_long(ValueBin),
    (Value bsr 1) bxor -(Value band 1).

pack_unsigned_long(Value) ->
    pack_unsigned_long(Value, <<>>).

pack_unsigned_long(Value, Acc) ->
    case Value band (bnot 16#7F) of
        0 ->
            <<Acc/binary, Value/integer>>;
        _ ->
            A = (Value band 16#7F) bor 16#80,
            pack_unsigned_long(Value bsr 7, <<Acc/binary, A/integer>>)
    end.

unpack_unsigned_long(Bin) ->
    unpack_unsigned_long(Bin, 0, 0).

unpack_unsigned_long(<<Byte:8/integer, Rest/binary>>, Shift, Result) when Shift < 64->
    Result2 = Result bor ((Byte band 16#7F) bsl Shift),
    case Byte band 16#80 of
        0 ->
            Result2;
        _ ->
            unpack_unsigned_long(Rest, Shift + 7, Result2)
    end.
