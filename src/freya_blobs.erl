%%%-------------------------------------------------------------------
%%% @doc
%%% Module with codecs for rowkey
%%% @end
%%%-------------------------------------------------------------------
-module(freya_blobs).

-include("freya.hrl").

-export([encode_rowkey/4, encode_timestamp/1, encode_value/2]).
-export([decode_rowkey/1, decode_timestamp/2, decode_value/2]).
-export([encode_search_key/2]).

%%%===================================================================
%%% API
%%%===================================================================
-spec encode_rowkey(metric_name(), milliseconds(), data_type(), data_tags()) ->
    {ok, binary()}.
encode_rowkey(MetricName, Ts, DataType, Tags) ->
    RowTime         = freya_utils:floor(Ts, ?ROW_WIDTH),
    DataTypeSize    = byte_size(DataType),
    {ok, TagsBin}   = pack_tags(Tags),
    Bin = <<MetricName/binary, 0:8/integer, RowTime:64/integer,
            0:8/integer, DataTypeSize:8/integer, DataType/binary,
            TagsBin/binary>>,
    {ok, Bin}.

-spec decode_rowkey(binary()) -> {ok, proplists:proplist()} | {error, any()}.
decode_rowkey(Bin0) when is_binary(Bin0) ->
    Fns = [fun decode_metric_name/1,
           fun decode_row_timestamp/1,
           fun decode_datatype/1,
           fun decode_tags/1],
    Result = hope_result:pipe(Fns, {[], Bin0}),
    case Result of
        {ok, {Parsed, _}} ->
            {ok, Parsed};
        {error, _} = Error ->
            Error
    end.

-spec encode_timestamp(milliseconds()) -> {ok, binary()}.
encode_timestamp(Ts) ->
    RowTime = freya_utils:floor(Ts, ?ROW_WIDTH),
    Offset = Ts - RowTime,
    {ok, <<Offset:31/integer, 0:1>>}.

-spec decode_timestamp(binary(), milliseconds()) -> {ok, milliseconds()}.
decode_timestamp(<<Offset:31/integer, _:1>>, RowTime) ->
    {ok, RowTime+Offset}.

-spec encode_value(binary(), any()) -> {ok, binary()}.
encode_value(<<"kairos_long">>, Value) ->
    {ok, pack_long(Value)};
encode_value(<<"kairos_legacy">>, Value) ->
    Long = pack_long(Value),
    {ok, <<0:8, Long/binary>>};
encode_value(<<"kairos_double">>, Value) ->
    {ok, <<Value/float>>};
encode_value(<<"kairos_string">>, Value) ->
    {ok, Value};
encode_value(<<"kairos_complex">>, {Real, Imag}) ->
    {ok, <<Real/float, Imag/float>>}.

-spec decode_value(any(), binary()) -> {ok, any()}.
decode_value(Value, <<"kairos_long">>) ->
    {ok, unpack_long(Value)};
decode_value(<<0:8, Value/binary>>, <<"kairos_legacy">>) ->
    {ok, unpack_long(Value)};
decode_value(<<Value/float>>, <<"kairos_double">>) ->
    {ok, Value};
decode_value(Value, <<"kairos_string">>) ->
    {ok, Value};
decode_value(<<Real/float, Imag/float>>, <<"kairos_complex">>) ->
    {ok, {Real, Imag}}.

-spec encode_search_key(metric_name(), milliseconds()) -> {ok, binary()}.
encode_search_key(MetricName, Ts) ->
    RowTime = freya_utils:floor(Ts, ?ROW_WIDTH),
    Bin = <<MetricName/binary, 0:8/integer, RowTime:64/integer>>,
    {ok, Bin}.

%%%===================================================================
%%% Internal
%%%===================================================================
decode_metric_name({Props, Bin}) ->
    case extract_metric_name(Bin, []) of
        {ok, {MetricName, Rest}} ->
            {ok, {[{name, MetricName}|Props], Rest}};
        {error, _} = Error ->
            Error
    end.

extract_metric_name(<<0, Rest/binary>>, Acc) ->
    {ok, {list_to_binary(lists:reverse(Acc)), Rest}};
extract_metric_name(<<H, Rest/binary>>, Acc) ->
    extract_metric_name(Rest, [H | Acc]);
extract_metric_name(<<>>, _Acc) ->
    {error, invalid_blob}.

decode_row_timestamp({Props, <<RowTime:64, Rest/binary>>}) ->
    {ok, {[{row_time, RowTime}|Props], Rest}}.

decode_datatype({Props, <<0, Len:8/integer, DataType:Len/binary-unit:8, Rest/binary>>}) ->
    {ok, {[{type, DataType}|Props], Rest}};
decode_datatype({Props, Rest}) ->
    {ok, {Props, Rest}}.

decode_tags({Props, <<>>=Bin}) ->
    {ok, {Props, Bin}};
decode_tags({Props, Bin}) ->
    {ok, Tags} = unpack_tags(Bin),
    {ok, {[{tags,Tags}|Props], Bin}}.

pack_tags([]) -> {ok, <<>>};
pack_tags(Tags) when is_list(Tags) ->
    KVs = lists:map(fun({K, V}) -> <<K/binary, "=", V/binary>> end, Tags),
    {ok, bstr:join(KVs, <<":">>)}.

unpack_tags(Bin) ->
    Res = lists:map(
            fun([K, V]) -> {K, V} end,
            lists:filtermap(
              fun(<<>>) -> false;
                 (KV) -> {true, binary:split(KV, <<"=">>)} end,
              binary:split(Bin, <<":">>, [global]))),
    {ok, Res}.

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
