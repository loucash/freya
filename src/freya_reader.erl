%%%-------------------------------------------------------------------
%%% @doc
%%% Querying
%%% @end
%%%-------------------------------------------------------------------
-module(freya_reader).

-include("freya.hrl").

-export([statements/0]).
-export([search/3, search/4, search/5]).

-export([do_search_data_points/3]).

% exported for tests
-export([read_row_size/0]).

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
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ?;">>},
     {?SELECT_ROWS_IN_RANGE,
      <<"SELECT rowkey FROM row_key_index WHERE "
        "metric_name = ? AND rowkey >= ? AND rowkey <= ?;">>},
     {?SELECT_DATA_FROM_START,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? LIMIT ?;">>},
     {?SELECT_DATA_IN_RANGE,
      <<"SELECT offset, value FROM data_points WHERE "
        "rowkey = ? AND offset >= ? AND offset <= ? LIMIT ?;">>}
    ].

-spec search(pool_name(), metric_name(), milliseconds()) -> {ok, list()}.
search(Pool, MetricName, StartTime) when is_atom(Pool),
                                         is_binary(MetricName),
                                         is_integer(StartTime) ->
    search(Pool, MetricName, StartTime, []).

-spec search(pool_name(), metric_name(), milliseconds(),
             milliseconds() | options()) -> {ok, list()}.
search(Pool, MetricName, StartTime, Options) when is_atom(Pool),
                                                  is_binary(MetricName),
                                                  is_integer(StartTime),
                                                  is_list(Options) ->
    case verify_options(Options, #search{metric_name=MetricName, start_time=StartTime}) of
        {ok, Search} ->
            do_search(Pool, Search);
        {error, _} = Error ->
            Error
    end;

search(Pool, MetricName, StartTime, EndTime) when is_atom(Pool),
                                                  is_binary(MetricName),
                                                  is_integer(StartTime),
                                                  is_integer(EndTime) ->
    search(Pool, MetricName, StartTime, EndTime, []).

-spec search(pool_name(), metric_name(), milliseconds(), milliseconds(), options()) -> {ok, list()}.
search(Pool, MetricName, StartTime, EndTime, Options) when is_atom(Pool),
                                                           is_binary(MetricName),
                                                           is_integer(StartTime),
                                                           is_integer(EndTime),
                                                           is_list(Options) ->
    case verify_options(Options, #search{metric_name=MetricName,
                                         start_time=StartTime,
                                         end_time=EndTime}) of
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
verify_options([{tags, Tags}|Options], Search) when is_list(Tags) ->
    verify_options(Options, Search#search{tags=Tags});
verify_options([Opt|_], _) ->
    {error, {bad_option, Opt}}.

read_consistency() ->
    A = freya:get_env(cassandra_read_consistency, quorum),
    {consistency, A}.

%% @doc Performs all steps of reading data from cassandra
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
do_search_rows(Client, #search{metric_name=MetricName,
                               start_time=StartTs, end_time=undefined}) ->
    {ok, StartTsBin} = freya_blobs:encode_search_key(MetricName, StartTs),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_FROM_START,
                          [MetricName, StartTsBin],
                          [read_consistency()]);
do_search_rows(Client, #search{metric_name=MetricName,
                            start_time=StartTs, end_time=EndTs}) ->
    {ok, StartTsBin} = freya_blobs:encode_search_key(MetricName, StartTs),
    {ok, EndTsBin}   = freya_blobs:encode_search_key(MetricName, EndTs),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_IN_RANGE,
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

do_search_data_points({RowKey, RowProps}, Pool, S) ->
    {ok, RowProps} = freya_blobs:decode_rowkey(RowKey),
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(Pool),
    Client = erlcql_cluster_worker:get_client(Worker),
    Result = query_data_points(Client, S, RowKey, RowProps),
    erlcql_cluster:checkin(Resource),
    Result.

query_data_points(Client, S, RowKey, RowProps) ->
    query_data_points(Client, S, RowKey, RowProps, []).

query_data_points(Client, #search{start_time=StartTime,
                                  end_time=undefined}=S, RowKey, RowProps, Acc) ->
    ReadRowSize = ?MODULE:read_row_size(),
    RowTime = proplists:get_value(row_time, RowProps),
    MaxTime = lists:max([RowTime, StartTime]),
    {ok, StartOffsetBin} = freya_blobs:encode_timestamp(MaxTime),
    QueryResult = erlcql_client:execute(
                    Client, ?SELECT_DATA_FROM_START,
                    [RowKey, StartOffsetBin, ReadRowSize],
                    [read_consistency()]),
    case QueryResult of
        {ok, {[], _}} -> {ok, lists:flatten(lists:reverse(Acc))};
        {ok, {BinDataPoints, _}} ->
            DataPoints = lists:map(to_data_point(RowProps), BinDataPoints),
            case length(DataPoints) == ReadRowSize of
                true ->
                    LastDP = lists:last(DataPoints),
                    NewStartTime = LastDP#data_point.ts+1,
                    query_data_points(Client,
                                      S#search{start_time=NewStartTime},
                                      RowKey, RowProps, [DataPoints|Acc]);
                false ->
                    {ok, lists:flatten(lists:reverse([DataPoints|Acc]))}
            end;
        {error, _} = Error ->
            Error
    end.

read_row_size() ->
    {ok, ReadRowSize} = freya:get_env(read_row_size),
    ReadRowSize.

to_data_point(RowProps0) ->
    RowTime  = proplists:get_value(row_time, RowProps0),
    DataType = proplists:get_value(type, RowProps0),
    fun([BinOffset, BinValue]) ->
        {ok, Ts} = freya_blobs:decode_timestamp(BinOffset, RowTime),
        {ok, Value} = freya_blobs:decode_value(BinValue, DataType),
        RowProps = [{ts, Ts},{value, Value}|RowProps0],
        freya_data_point:of_props(RowProps)
    end.
