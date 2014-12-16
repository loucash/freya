-module(freya_blobs).

-include("freya.hrl").

-export([encode/1,
         decode/3]).

%%%===================================================================
%%% API
%%%===================================================================
-spec decode(binary(), binary(), binary()) ->
    {ok, data_point()} | {error, any()}.
decode(Row, Timestamp, Value) ->
    Fns = [fun(DataPoint) -> decode_rowkey(Row, DataPoint) end,
           fun(DataPoint) -> decode_timestamp(Timestamp, DataPoint) end,
           fun(DataPoint) -> decode_value(Value, DataPoint) end],
    hope_result:pipe(Fns, #data_point{}).

-spec encode(data_point()) -> {ok, {binary(), binary(), binary()}}.
encode(#data_point{}=DataPoint) ->
    {ok, Row}       = encode_rowkey(DataPoint),
    {ok, Timestamp} = encode_timestamp(DataPoint),
    {ok, Value}     = encode_value(DataPoint),
    {ok, {Row, Timestamp, Value}}.

%%%===================================================================
%%% Internal
%%%===================================================================
encode_rowkey(#data_point{name=MetricName, ts=Ts, type=DataType, tags=Tags}) ->
    RowTime = freya_utils:floor(Ts, ?ROW_WIDTH),
    DataTypeSize = byte_size(DataType),
    TagsBin = pack_tags(Tags),
    Bin = <<MetricName/binary, 0:8/integer, RowTime:64/integer,
            0:8/integer, DataTypeSize:8/integer, DataType/binary,
            TagsBin/binary>>,
    {ok, Bin}.

-spec decode_rowkey(binary(), data_point()) -> {ok, data_point()}.
decode_rowkey(Bin0, DataPoint0) when is_binary(Bin0) ->
    Fns = [fun decode_metric_name/1,
           fun decode_row_timestamp/1,
           fun decode_datatype/1,
           fun decode_tags/1],
    Result = hope_result:pipe(Fns, {DataPoint0, Bin0}),
    case Result of
        {ok, {DataPoint2, _}} ->
            {ok, DataPoint2};
        {error, _} = Error ->
            Error
    end.

decode_metric_name({DataPoint, Bin}) ->
    {MetricName, Rest} = extract_metric_name(Bin, []),
    {ok, {DataPoint#data_point{name=MetricName}, Rest}}.

extract_metric_name(<<0, Rest/binary>>, Acc) ->
    {list_to_binary(lists:reverse(Acc)), Rest};
extract_metric_name(<<H, Rest/binary>>, Acc) ->
    extract_metric_name(Rest, [H | Acc]).

decode_row_timestamp({DataPoint, <<RowTime:64, Rest/binary>>}) ->
    {ok, {DataPoint#data_point{row_time=RowTime}, Rest}}.

decode_datatype({DataPoint, <<0, Len:8/integer, DataType:Len/binary-unit:8, Rest/binary>>}) ->
    {ok, {DataPoint#data_point{type=DataType}, Rest}};
decode_datatype({DataPoint, Rest}) ->
    {ok, {DataPoint, Rest}}.

decode_tags({DataPoint, <<>>=Bin}) ->
    {ok, {DataPoint, Bin}};
decode_tags({DataPoint, Bin}) ->
    Tags = unpack_tags(Bin),
    {ok, {DataPoint#data_point{tags=Tags}, Bin}}.

unpack_tags(Bin) ->
    lists:map(
      fun([K, V]) -> {K, V} end,
      lists:filtermap(
        fun(<<>>) -> false;
           (KV) -> {true, binary:split(KV, <<"=">>)} end,
        binary:split(Bin, <<":">>, [global]))).

pack_tags([]) -> <<>>;
pack_tags(Tags) when is_list(Tags) ->
    KVs = lists:map(fun({K, V}) -> <<K/binary, "=", V/binary>> end, Tags),
    bstr:join(KVs, <<":">>).

encode_timestamp(#data_point{ts=Ts}) ->
    RowTime = freya_utils:floor(Ts, ?ROW_WIDTH),
    Offset = Ts - RowTime,
    {ok, <<Offset:31/integer, 0:1>>}.

decode_timestamp(<<Offset:31/integer, _:1>>, #data_point{row_time=RowTime}=DataPoint) ->
    {ok, DataPoint#data_point{ts=RowTime+Offset}}.

encode_value(#data_point{type = <<"kairos_long">>, value=Value}) ->
    {ok, binary:encode_unsigned(Value)}.

decode_value(Value, #data_point{type = <<"kairos_long">>}=DataPoint) ->
    {ok, DataPoint#data_point{value=binary:decode_unsigned(Value)}}.
