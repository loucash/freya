%%%-------------------------------------------------------------------
%%% @doc
%%% Data point functions
%%% @end
%%%-------------------------------------------------------------------
-module(freya_data_point).

-export([new/0, new/3, new/4, of_props/1]).
-export([encode/1, encode/2, decode/3]).
-export([ns/1, name/1, tags/1, ts/1, value/1]).

-include("freya.hrl").

-define(blobs, freya_blobs).

%%%===================================================================
%%% API
%%%===================================================================
-spec new() -> data_point().
new() ->
    #data_point{}.

-spec new(metric(), milliseconds(), data_value()) -> data_point().
new(Metric, Ts, Value) ->
    new(Metric, Ts, Value, []).

-spec new(metric(), milliseconds(), data_value(), data_tags()) -> data_point().
new(Metric, Ts, Value, Tags) ->
    new(Metric, Ts, Value, Tags, []).

-spec new(metric(), milliseconds(), data_value(), data_tags(), data_meta()) -> data_point().
new({Ns, Name}, Ts, Value, Tags, _Meta) ->
    #data_point{ns    = Ns,
                name  = Name,
                ts    = Ts,
                type  = type(Value),
                value = Value,
                tags  = freya_utils:sanitize_tags(Tags)};
new(Name, Ts, Value, Tags, Meta) when is_binary(Name) ->
    new(freya_utils:sanitize_name(Name), Ts, Value, Tags, Meta).

-spec of_props(proplists:proplist()) -> data_point().
of_props(Props) ->
    Get2 = fun proplists:get_value/2,
    Get3 = fun proplists:get_value/3,
    #data_point{
       ns       = Get2(ns, Props),
       name     = Get2(name, Props),
       type     = Get2(type, Props),
       tags     = Get3(tags, Props, []),
       ts       = Get2(ts, Props),
       value    = Get2(value, Props)
      }.

-spec name(data_point()) -> metric_name().
name(#data_point{name=Name}) ->
    Name.

-spec ns(data_point()) -> metric_ns().
ns(#data_point{ns=Ns}) ->
    Ns.

-spec tags(data_point()) -> data_tags().
tags(#data_point{tags=Tags}) ->
    Tags.

-spec ts(data_point()) -> milliseconds().
ts(#data_point{ts=Ts}) ->
    Ts.

-spec value(data_point()) -> data_value().
value(#data_point{value=Value}) ->
    Value.

-spec decode(binary(), binary(), binary()) ->
    {ok, data_point()} | {error, any()}.
decode(Row, Timestamp, Value) ->
    Fns = [fun(DataPoint) -> decode_rowkey(Row, DataPoint) end,
           fun({DataPoint, RowTime}) ->
                decode_timestamp(RowTime, Timestamp, DataPoint)
           end,
           fun(DataPoint) -> decode_value(Value, DataPoint) end],
    hope_result:pipe(Fns, new()).

-spec encode(data_point()) -> {ok, {binary(), binary(), binary()}}.
encode(DataPoint) ->
    encode(DataPoint, raw).

-spec encode(data_point(), data_precision()) ->
    {ok, {binary(), binary(), binary()}}.
encode(#data_point{ns=Ns, name=Name, ts=Ts, type=DataType, tags=Tags, value=Value0},
       DataPrecision) ->
    {ok, Row}       = ?blobs:encode_rowkey({Ns, Name}, Ts, DataType, Tags, DataPrecision),
    {ok, Timestamp} = ?blobs:encode_timestamp(Ts, DataPrecision),
    {ok, Value}     = ?blobs:encode_value(DataType, Value0),
    {ok, {Row, Timestamp, Value}}.

%%%===================================================================
%%% Internal
%%%===================================================================
-spec decode_rowkey(binary(), data_point()) -> {ok, {data_point(), milliseconds()}}
                                             | {error, invalid}.
decode_rowkey(Bin0, DataPoint0) when is_binary(Bin0) ->
    Get2 = fun proplists:get_value/2,
    Get3 = fun proplists:get_value/3,
    case ?blobs:decode_rowkey(Bin0) of
        {ok, Props} ->
            {ok, {DataPoint0#data_point{
                    ns       = Get2(ns, Props),
                    name     = Get2(name, Props),
                    type     = Get2(type, Props),
                    tags     = Get3(tags, Props, [])
                   },
                  Get2(row_time, Props)}};
        {error, _} = Error ->
            Error
    end.

-spec decode_timestamp(milliseconds(), binary(), data_point()) -> {ok, data_point()}.
decode_timestamp(RowTime, Bin, #data_point{}=DataPoint) ->
    {ok, Ts} = ?blobs:decode_timestamp(Bin, RowTime),
    {ok, DataPoint#data_point{ts=Ts}}.

-spec decode_value(binary(), data_point()) -> {ok, data_point()}.
decode_value(Val0, #data_point{type=Type}=DataPoint) ->
    {ok, Val} = ?blobs:decode_value(Val0, Type),
    {ok, DataPoint#data_point{value=Val}}.

type(Value) when is_integer(Value) -> long;
type(Value) when is_float(Value)   -> double.
