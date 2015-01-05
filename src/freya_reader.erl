%%%-------------------------------------------------------------------
%%% @doc
%%% Querying
%%% @end
%%%-------------------------------------------------------------------
-module(freya_reader).

-include("freya.hrl").

-export([statements/0]).
-export([search/3, search/4, search/5]).

-type option()  :: {aggregate, aggregate()} |
                   {precision, precision()} |
                   {aligned,   boolean()}.
-type options() :: [option()].

-record(search, {
          metric_name       :: binary(),
          start_time        :: milliseconds(),
          end_time          :: milliseconds() | undefined,
          aligned = false   :: boolean(),
          aggregate         :: aggregate(),
          precision         :: precision(),
          tags              :: proplists:proplist()
         }).
-type search()  :: #search{}.


%%%===================================================================
%%% API
%%%===================================================================
statements() ->
    [
     {?SELECT_ROWS_FROM_START,
      <<"SELECT rowkey FROM row_key_index WHERE metric_name = ? AND rowkey >= ?;">>},
     {?SELECT_ROWS_IN_RANGE,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ? AND rowkey <= ?;">>},
     {?SELECT_DATA_FROM_START,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ?;">>},
     {?SELECT_DATA_IN_RANGE,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? AND offset <= ?;">>}
    ].

-spec search(pid(), metric_name(), milliseconds()) -> {ok, list()}.
search(Client, MetricName, StartTime) when is_pid(Client),
                                           is_binary(MetricName),
                                           is_integer(StartTime) ->
    search(Client, MetricName, StartTime, []).

-spec search(pid(), metric_name(), milliseconds(),
             milliseconds() | options()) -> {ok, list()}.
search(Client, MetricName, StartTime, Options) when is_pid(Client),
                                                    is_binary(MetricName),
                                                    is_integer(StartTime),
                                                    is_list(Options) ->
    case verify_options(Options, #search{metric_name=MetricName, start_time=StartTime}) of
        {ok, Search} ->
            do_search(Client, Search);
        {error, _} = Error ->
            Error
    end;

search(Client, MetricName, StartTime, EndTime) when is_pid(Client),
                                                    is_binary(MetricName),
                                                    is_integer(StartTime),
                                                    is_integer(EndTime) ->
    search(Client, MetricName, StartTime, EndTime, []).

-spec search(pid(), metric_name(), milliseconds(), milliseconds(), options()) -> {ok, list()}.
search(Client, MetricName, StartTime, EndTime, Options) when is_pid(Client),
                                                             is_binary(MetricName),
                                                             is_integer(StartTime),
                                                             is_integer(EndTime),
                                                             is_list(Options) ->
    case verify_options(Options, #search{metric_name=MetricName,
                                         start_time=StartTime,
                                         end_time=EndTime}) of
        {ok, Search} ->
            do_search(Client, Search);
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Internal
%%%===================================================================
-spec verify_options(options(), search()) -> {ok, search()} | {error, any()}.
verify_options([], Search) ->
    {ok, Search};
verify_options([{aligned, Value}|Options], Search) when is_boolean(Value) ->
    verify_options(Options, Search#search{aligned=Value});
verify_options([{aggregate, Value}|Options], Search) ->
    case lists:member(Value, ?AGGREGATES) of
        true ->
            verify_options(Options, Search#search{aggregate=Value});
        false ->
            {error, bad_aggregate}
    end;
verify_options([{precision, {Val, Type}}|Options], Search) when is_integer(Val) ->
    case lists:member(Type, ?UNITS) of
        true ->
            verify_options(Options, Search#search{precision={Val, Type}});
        false ->
            {error, bad_precision}
    end;
verify_options([Opt|_], _) ->
    {error, {bad_option, Opt}}.

consistency() ->
    A = freya:get_env(cassandra_read_consistency, quorum),
    {consistency, A}.

do_search(Client, #search{}=S) ->
    Fns = [
        fun(_)       -> search_rows(Client, S)                  end,
        fun(RowKeys) -> search_data_points(Client, S, RowKeys)  end
    ],
    hope_result:pipe(Fns, undefined).

search_rows(Client, Search) ->
    case do_search_rows(Client, Search) of
        {ok, {[], _}} -> {error, not_found};
        {ok, {RowKeys, _}} -> {ok, lists:flatten(RowKeys)};
        {error, _} = Error -> Error
    end.

do_search_rows(Client, #search{metric_name=MetricName,
                               start_time=StartTs, end_time=undefined}) ->
    {ok, StartTsBin} = freya_blobs:encode_search_key(MetricName, StartTs),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_FROM_START,
                          [MetricName, StartTsBin],
                          [consistency()]);
do_search_rows(Client, #search{metric_name=MetricName,
                            start_time=StartTs, end_time=EndTs}) ->
    {ok, StartTsBin} = freya_blobs:encode_search_key(MetricName, StartTs),
    {ok, EndTsBin}   = freya_blobs:encode_search_key(MetricName, EndTs),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_IN_RANGE,
                          [MetricName, StartTsBin, EndTsBin],
                          [consistency()]).

search_data_points(Client, #search{}=S, RowKeys) ->
    do_search_data_points(Client, S, RowKeys, []).

do_search_data_points(_Client, _S, [], []) ->
    {error, not_found};
do_search_data_points(_Client, _S, [], Acc) ->
    {ok, lists:flatten(lists:reverse(Acc))};
do_search_data_points(Client, S, [RowKey|RowKeys], Acc) ->
    {ok, RowProps} = freya_blobs:decode_rowkey(RowKey),
    case query_data_points(Client, S, RowKey, RowProps) of
        {ok, DataPoints} ->
            do_search_data_points(Client, S, RowKeys, [DataPoints|Acc]);
        {error, _} = Error ->
            Error
    end.

query_data_points(Client, #search{start_time=StartTime,
                                  end_time=undefined}, RowKey, RowProps) ->
    RowTime = proplists:get_value(row_time, RowProps),
    MaxTime = lists:max([RowTime, StartTime]),
    {ok, StartOffsetBin} = freya_blobs:encode_timestamp(MaxTime),
    case erlcql_client:execute(Client,
                               ?SELECT_DATA_FROM_START,
                               [RowKey, StartOffsetBin],
                               [consistency()]) of
        {ok, {[], _}} -> [];
        {ok, {BinDataPoints, _}} ->
            DataPoints = lists:map(to_data_point(RowProps), BinDataPoints),
            {ok, DataPoints};
        {error, _} = Error ->
            Error
    end.

to_data_point(RowProps0) ->
    RowTime  = proplists:get_value(row_time, RowProps0),
    DataType = proplists:get_value(type, RowProps0),
    fun([BinOffset, BinValue]) ->
        {ok, Ts} = freya_blobs:decode_timestamp(BinOffset, RowTime),
        {ok, Value} = freya_blobs:decode_value(BinValue, DataType),
        RowProps = [{ts, Ts},{value, Value}|RowProps0],
        freya_data_point:of_props(RowProps)
    end.
