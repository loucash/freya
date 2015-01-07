%%%-------------------------------------------------------------------
%%% @doc
%%% Data point functions
%%% @end
%%%-------------------------------------------------------------------
-module(freya_data_point).
-include("freya.hrl").

-export([new/0, new/4, new/5, of_props/1]).
-export([encode/1, decode/3]).

-define(blobs, freya_blobs).

%%%===================================================================
%%% API
%%%===================================================================
-spec new() -> data_point().
new() ->
    #data_point{}.

-spec new(metric_name(), milliseconds(), data_type(), data_value()) -> data_point().
new(MetricName, Ts, Type, Value) ->
    new(MetricName, Ts, Type, Value, []).

-spec new(metric_name(), milliseconds(), data_type(), data_value(), data_tags()) -> data_point().
new(MetricName, Ts, Type, Value, Tags) ->
    #data_point{name        = MetricName,
                ts          = Ts,
                type        = Type,
                value       = Value,
                tags        = Tags,
                row_time    = freya_utils:floor(Ts, ?ROW_WIDTH)}.

-spec of_props(proplists:proplist()) -> data_point().
of_props(Props) ->
    Get2 = fun proplists:get_value/2,
    Get3 = fun proplists:get_value/3,
    #data_point{
       name     = Get2(name, Props),
       row_time = Get2(row_time, Props),
       type     = Get2(type, Props),
       tags     = Get3(tags, Props, []),
       ts       = Get2(ts, Props),
       value    = Get2(value, Props)
      }.

-spec decode(binary(), binary(), binary()) ->
    {ok, data_point()} | {error, any()}.
decode(Row, Timestamp, Value) ->
    Fns = [fun(DataPoint) -> decode_rowkey(Row, DataPoint) end,
           fun(DataPoint) -> decode_timestamp(Timestamp, DataPoint) end,
           fun(DataPoint) -> decode_value(Value, DataPoint) end],
    hope_result:pipe(Fns, new()).

-spec encode(data_point()) -> {ok, {binary(), binary(), binary()}}.
encode(#data_point{name=MetricName, ts=Ts, type=DataType, tags=Tags, value=Value0}) ->
    {ok, Row}       = ?blobs:encode_rowkey(MetricName, Ts, DataType, Tags),
    {ok, Timestamp} = ?blobs:encode_timestamp(Ts),
    {ok, Value}     = ?blobs:encode_value(DataType, Value0),
    {ok, {Row, Timestamp, Value}}.

%%%===================================================================
%%% Internal
%%%===================================================================
-spec decode_rowkey(binary(), data_point()) -> {ok, data_point()}.
decode_rowkey(Bin0, DataPoint0) when is_binary(Bin0) ->
    Get2 = fun proplists:get_value/2,
    Get3 = fun proplists:get_value/3,
    case ?blobs:decode_rowkey(Bin0) of
        {ok, Props} ->
            {ok, DataPoint0#data_point{
                   name     = Get2(name, Props),
                   row_time = Get2(row_time, Props),
                   type     = Get2(type, Props),
                   tags     = Get3(tags, Props, [])
                  }};
        {error, _} = Error ->
            Error
    end.

-spec decode_timestamp(binary(), data_point()) -> {ok, data_point()}.
decode_timestamp(Bin, #data_point{row_time=RowTime}=DataPoint) ->
    {ok, Ts} = ?blobs:decode_timestamp(Bin, RowTime),
    {ok, DataPoint#data_point{ts=Ts}}.

-spec decode_value(binary(), data_point()) -> {ok, data_point()}.
decode_value(Val0, #data_point{type=Type}=DataPoint) ->
    {ok, Val} = ?blobs:decode_value(Val0, Type),
    {ok, DataPoint#data_point{value=Val}}.
