-module(freya_dm_codecs).

-export([new_rowkey/3, new_rowkey/4]).
-export([decode_rowkey/1, encode_rowkey/1]).

-record(rowkey,
        {name           :: binary(),
         ts             :: non_neg_integer(),
         data_type      :: binary(),
         tags = []      :: proplists:proplist()}).
-opaque rowkey() :: #rowkey{}.
-export_type([rowkey/0]).

-define(DATA_TYPES,
        [<<"kairos_complex">>,
         <<"kairos_double">>,
         <<"kairos_legacy">>,
         <<"kairos_long">>,
         <<"kairos_string">>]).

%%%===================================================================
%%% API
%%%===================================================================
-spec decode_rowkey(binary()) -> {ok, rowkey()}.
decode_rowkey(Bin0) when is_binary(Bin0) ->
    {RowKey, _} = lists:foldl(fun(F, {RowKey, Bin}) -> F(RowKey, Bin) end,
                              {new_rowkey(), Bin0},
                              [fun decode_metric_name/2,
                               fun decode_row_timestamp/2,
                               fun decode_datatype/2,
                               fun decode_tags/2]),
    {ok, RowKey}.

-spec encode_rowkey(rowkey()) -> {ok, binary()}.
encode_rowkey(#rowkey{name=MetricName, ts=Ts, data_type=DataType, tags=Tags}) ->
    DataTypeSize = byte_size(DataType),
    TagsBin = encode_tags(Tags),
    Bin = <<MetricName/binary, 0:8/integer, Ts:64/integer,
            0:8/integer, DataTypeSize:8/integer, DataType/binary,
            TagsBin/binary>>,
    {ok, Bin}.

-spec new_rowkey(binary(), non_neg_integer(), binary()) -> rowkey().
new_rowkey(MetricName, Timestamp, DataType) ->
    new_rowkey(MetricName, Timestamp, DataType, []).

-spec new_rowkey(binary(), non_neg_integer(), binary(), proplists:proplist()) ->
    rowkey().
new_rowkey(MetricName, Timestamp, DataType, Tags) ->
    #rowkey{name=MetricName, ts=Timestamp, data_type=DataType, tags=Tags}.

%%%===================================================================
%%% Internal
%%%===================================================================
decode_metric_name(RowKey, Bin) ->
    {MetricName, Rest} = extract_metric_name(Bin, []),
    {RowKey#rowkey{name=MetricName}, Rest}.

extract_metric_name(<<0, Rest/binary>>, Acc) ->
    {list_to_binary(lists:reverse(Acc)), Rest};
extract_metric_name(<<H, Rest/binary>>, Acc) ->
    extract_metric_name(Rest, [H | Acc]).

decode_row_timestamp(RowKey, <<Ts:64, Rest/binary>>) ->
    {RowKey#rowkey{ts=Ts}, Rest}.

decode_datatype(RowKey, <<0, Len:8/integer, DataType:Len/binary-unit:8, Rest/binary>>) ->
    {RowKey#rowkey{data_type=DataType}, Rest};
decode_datatype(RowKey, Rest) ->
    {RowKey, Rest}.

decode_tags(RowKey, <<>>=Bin) ->
    {RowKey, Bin};
decode_tags(RowKey, Bin) ->
    Tags = decode_tags(Bin),
    {RowKey#rowkey{tags=Tags}, Bin}.

decode_tags(Bin) ->
    lists:map(
      fun([K, V]) -> {K, V} end,
      lists:filtermap(
        fun(<<>>) -> false;
           (KV) -> {true, binary:split(KV, <<"=">>)} end,
        binary:split(Bin, <<":">>, [global]))).

encode_tags([]) -> <<>>;
encode_tags(Tags) when is_list(Tags) ->
    KVs = lists:map(fun({K, V}) -> <<K/binary, "=", V/binary>> end, Tags),
    bstr:join(KVs, <<":">>).

new_rowkey() ->
    #rowkey{}.
