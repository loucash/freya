%%%-------------------------------------------------------------------
%%% @doc
%%% Querying
%%% @end
%%%-------------------------------------------------------------------
-module(freya_reader).

-include("freya.hrl").

-export([statements/0]).
-export([search/2]).

-export([do_search_data_points/3]).

% exported for tests
-export([read_row_size/0]).

-type option()  :: {metric_name, binary()} |
                   {start_time, milliseconds()} |
                   {end_time, milliseconds()} |
                   {aligned,   boolean()} |
                   {aggregate, aggregate()} |
                   {precision, precision()} |
                   {tags, proplists:proplist()} |
                   {order, data_order()}.
-type options() :: [option()].

-record(search, {
          metric_name       :: binary(),
          start_time        :: milliseconds(),
          end_time          :: milliseconds() | undefined,
          aligned = false   :: boolean(),
          aggregate         :: aggregate(),
          precision         :: precision(),
          tags              :: proplists:proplist(),
          order = asc       :: data_order()
         }).
-type search()  :: #search{}.

%%%===================================================================
%%% API
%%%===================================================================
statements() ->
    [
     {?SELECT_ROWS_FROM_START_ASC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ? ORDER BY rowkey ASC;">>},
     {?SELECT_ROWS_FROM_START_DESC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ? ORDER BY rowkey DESC;">>},
     {?SELECT_ROWS_IN_RANGE_ASC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ? AND rowkey <= ? ORDER BY rowkey ASC;">>},
     {?SELECT_ROWS_IN_RANGE_DESC,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ? AND rowkey <= ? ORDER BY rowkey DESC;">>},
     {?SELECT_DATA_FROM_START_ASC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? ORDER BY offset ASC  LIMIT ?;">>},
     {?SELECT_DATA_FROM_START_DESC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? ORDER BY offset DESC LIMIT ?;">>},
     {?SELECT_DATA_IN_RANGE_ASC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? AND offset <= ? LIMIT ?;">>},
     {?SELECT_DATA_IN_RANGE_DESC,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? AND offset <= ? ORDER BY offset DESC LIMIT ?;">>}
    ].

-spec search(pool_name(), options()) -> {ok, list()} | {error, any()}.
search(Pool, Options) when is_list(Options) ->
    case verify_options(Options, #search{}) of
        {ok, Search} ->
            do_search(Pool, Search);
        {error, _} = Error ->
            Error
    end.

%%%===================================================================
%%% Internal
%%%===================================================================
-spec verify_options(options(), search()) -> {ok, search()} | {error, any()}.
verify_options([], Search) ->
    {ok, Search};
verify_options([{metric_name, MetricName}|Options], Search) when is_binary(MetricName) ->
    verify_options(Options, Search#search{metric_name=MetricName});
verify_options([{start_time, Start}|Options], Search) when is_integer(Start) ->
    verify_options(Options, Search#search{start_time=Start});
verify_options([{end_time, End}|Options], Search) when is_integer(End) ->
    verify_options(Options, Search#search{end_time=End});
verify_options([{aligned, Bool}|Options], Search) when is_boolean(Bool) ->
    verify_options(Options, Search#search{aligned=Bool});
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
verify_options([{tags, List}|Options], Search) when is_list(List) ->
    verify_options(Options, Search#search{tags=List});
verify_options([{order, Order}|Options], Search) when Order =:= asc orelse Order =:= desc ->
    verify_options(Options, Search#search{order=Order});
verify_options([Opt|_], _Search) ->
    {error, {bad_option, Opt}}.

read_consistency() ->
    A = freya:get_env(cassandra_read_consistency, quorum),
    {consistency, A}.

%% @doc Performs all steps of reading data from cassandra
do_search(_Pool, #search{metric_name=undefined}) ->
    {error, {missing_param, metric_name}};
do_search(_Pool, #search{start_time=undefined}) ->
    {error, {missing_param, start_time}};
do_search(_Pool, #search{start_time=ST, end_time=ET}) when ET =/= undefined andalso
                                                           ST > ET ->
    {error, invalid_time_range};
do_search(Pool, #search{}=S) ->
    Fns = [
        fun(_)    -> search_rows(Pool, S) end,
        fun(Rows) -> decode_rowkeys(Rows) end,
        fun(Rows) -> filter_row_tags(S, Rows) end,
        fun(Rows) -> search_data_points(Pool, S, Rows) end
    ],
    hope_result:pipe(Fns, undefined).

%% @doc Return rowkeys indexes in row_key_index
search_rows(Pool, Search) ->
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(Pool),
    Client = erlcql_cluster_worker:get_client(Worker),
    Result = do_search_rows(Client, Search),
    erlcql_cluster:checkin(Resource),
    case Result of
        {ok, {[], _}} -> {error, not_found};
        {ok, {RowKeys, _}} -> {ok, lists:flatten(RowKeys)};
        {error, _} = Error -> Error
    end.

%% @doc Return query execute result
do_search_rows(Client, #search{metric_name=MetricName, order=Order,
                               start_time=StartTs, end_time=undefined}) ->
    StartRowTime = freya_utils:floor(StartTs, ?ROW_WIDTH),
    {ok, StartTsBin} = freya_blobs:encode_search_key(MetricName, StartRowTime),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_FROM_START(Order),
                          [MetricName, StartTsBin],
                          [read_consistency()]);
do_search_rows(Client, #search{metric_name=MetricName, order=Order,
                               start_time=StartTs, end_time=EndTs}) ->
    StartRowTime = freya_utils:floor(StartTs, ?ROW_WIDTH),
    EndRowTime   = freya_utils:floor(EndTs, ?ROW_WIDTH) + 1,
    {ok, StartTsBin} = freya_blobs:encode_search_key(MetricName, StartRowTime),
    {ok, EndTsBin}   = freya_blobs:encode_search_key(MetricName, EndRowTime),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_IN_RANGE(Order),
                          [MetricName, StartTsBin, EndTsBin],
                          [read_consistency()]).

%% @doc Return tuples with original and decoded rowkey
decode_rowkeys(RowKeys) ->
    Rows = lists:map(
             fun(RowKey) ->
                {ok, RowProps} = freya_blobs:decode_rowkey(RowKey),
                {RowKey, RowProps}
             end, RowKeys),
    {ok, Rows}.

%% @doc Drop rows that does not match tags in a search query
filter_row_tags(#search{tags=[]}, Rows) -> {ok, Rows};
filter_row_tags(#search{tags=undefined}, Rows) -> {ok, Rows};
filter_row_tags(#search{tags=Tags}, Rows) ->
    {ok, lists:filter(match_row_fun(Tags), Rows)}.

%% @doc Return a function to filter each rowkey
match_row_fun(Tags) ->
    MatchTagFuns = lists:map(fun match_tag_fun/1, Tags),
    fun({_RowKey, RowProps}) ->
        RowTags = proplists:get_value(tags, RowProps),
        lists:all(matching_tags(RowTags), MatchTagFuns)
    end.

%% @doc Return a function to match two tags
match_tag_fun({FilterKey, FilterValue}) ->
    fun({Key, Value}) when FilterKey   =:= Key andalso
                           FilterValue =:= Value -> true;
       (_) -> false
    end.

%% @doc Return a function to match at row tags
matching_tags(RowTags) ->
    fun(MatchFun) ->
        lists:any(MatchFun, RowTags)
    end.

%% @doc Return result of querying each rowkey, done in parallel using pmap
search_data_points(Pool, #search{}=S, Rows) ->
    Results0 = rpc:pmap({?MODULE, do_search_data_points},
                        [Pool, S], Rows),
    Results1 = lists:foldl(fun({error, _}=Error, _) -> Error;
                              (_, {error, _}=Error) -> Error;
                              ({ok, DataPoints}, Acc) -> [DataPoints|Acc] end,
                           [], Results0),
    case Results1 of
        {error, _} = Error -> Error;
        _ -> {ok, lists:flatten(lists:reverse(Results1))}
    end.

do_search_data_points(Row, Pool, S) ->
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(Pool),
    Client = erlcql_cluster_worker:get_client(Worker),
    Result = query_data_points(Row, Client, S),
    erlcql_cluster:checkin(Resource),
    Result.

query_data_points(Row, Client, S) ->
    query_data_points(Row, Client, S, []).

query_data_points({RowKey, RowProps}=Row, Client,
                  #search{start_time=StartTime, end_time=undefined,
                          order=Order}=S, Acc) ->
    ReadRowSize     = ?MODULE:read_row_size(),
    StartOffsetBin  = start_time_bin(StartTime, RowProps),
    QueryResult     = erlcql_client:execute(
                        Client, ?SELECT_DATA_FROM_START(Order),
                        [RowKey, StartOffsetBin, ReadRowSize],
                        [read_consistency()]),
    case QueryResult of
        {ok, {[], _}} -> {ok, lists:flatten(lists:reverse(Acc))};
        {ok, {BinDataPoints, _}} ->
            DataPoints = lists:map(to_data_point(RowProps), BinDataPoints),
            case length(DataPoints) == ReadRowSize of
                true ->
                    continue_query_data_points(DataPoints, Row, Client, S, Acc);
                false ->
                    {ok, lists:flatten(lists:reverse([DataPoints|Acc]))}
            end;
        {error, _} = Error ->
            Error
    end;
query_data_points({RowKey, RowProps}=Row, Client,
                  #search{start_time=StartTime, end_time=EndTime,
                          order=Order}=S, Acc) ->
    ReadRowSize     = ?MODULE:read_row_size(),
    StartOffsetBin  = start_time_bin(StartTime, RowProps),
    EndOffsetBin    = end_time_bin(EndTime, RowProps),
    QueryResult     = erlcql_client:execute(
                        Client, ?SELECT_DATA_IN_RANGE(Order),
                        [RowKey, StartOffsetBin, EndOffsetBin, ReadRowSize],
                        [read_consistency()]),
    case QueryResult of
        {ok, {[], _}} -> {ok, lists:flatten(lists:reverse(Acc))};
        {ok, {BinDataPoints, _}} ->
            DataPoints = lists:map(to_data_point(RowProps), BinDataPoints),
            case length(DataPoints) == ReadRowSize of
                true ->
                    continue_query_data_points(DataPoints, Row, Client, S, Acc);
                false ->
                    {ok, lists:flatten(lists:reverse([DataPoints|Acc]))}
            end;
        {error, _} = Error ->
            Error
    end.

continue_query_data_points(DataPoints, Row, Client, #search{order=asc}=S, Acc) ->
    LastDP = lists:last(DataPoints),
    NewStartTime = LastDP#data_point.ts+1,
    query_data_points(Row, Client,
                      S#search{start_time=NewStartTime},
                      [DataPoints|Acc]);
continue_query_data_points(DataPoints, Row, Client, #search{order=desc}=S, Acc) ->
    LastDP = lists:last(DataPoints),
    NewEndTime = LastDP#data_point.ts-1,
    query_data_points(Row, Client,
                      S#search{end_time=NewEndTime},
                      [DataPoints|Acc]).

%% @doc Return binary format of start time offset
start_time_bin(StartTime, RowProps) ->
    RowTime = proplists:get_value(row_time, RowProps),
    MaxTime = lists:max([RowTime, StartTime]),
    {ok, Bin} = freya_blobs:encode_timestamp(MaxTime),
    Bin.

%% @doc Return binary format of end time offset
end_time_bin(EndTime, RowProps) ->
    RowTime = proplists:get_value(row_time, RowProps),
    RowWidthMs = freya_utils:ms(?ROW_WIDTH),
    {ok, Bin} = case EndTime > (RowTime + RowWidthMs) of
                    true ->
                        freya_blobs:encode_offset(RowWidthMs+1);
                    false ->
                        freya_blobs:encode_timestamp(EndTime)
                end,
    Bin.

%% @doc Read configuration parameter: read_row_size
read_row_size() ->
    {ok, ReadRowSize} = freya:get_env(read_row_size),
    ReadRowSize.

%% @doc Return a function that can create #data_point
to_data_point(RowProps0) ->
    RowTime  = proplists:get_value(row_time, RowProps0),
    DataType = proplists:get_value(type, RowProps0),
    fun([BinOffset, BinValue]) ->
        {ok, Ts} = freya_blobs:decode_timestamp(BinOffset, RowTime),
        {ok, Value} = freya_blobs:decode_value(BinValue, DataType),
        RowProps = [{ts, Ts},{value, Value}|RowProps0],
        freya_data_point:of_props(RowProps)
    end.
