%%%-------------------------------------------------------------------
%%% @doc
%%% Querying
%%% @end
%%%-------------------------------------------------------------------
-module(freya_reader).

-include("freya.hrl").
-include("freya_reader.hrl").

-export([statements/0]).
-export([search/1, search/2]).
-export([metric_names/1, metric_names/2]).
-export([namespaces/0, namespaces/1]).

% exported for tests
-export([read_row_size/0]).

-type option()  :: {ns, metric_ns()} |
                   {name, metric_name()} |
                   {start_time, milliseconds()} |
                   {end_time, milliseconds()} |
                   {align, boolean()} |
                   {aggregate, {aggregate(), precision()}} |
                   {source, data_precision()} |
                   {tags, proplists:proplist()} |
                   {order, data_order()}.
-type options() :: [option()].

-record(search, {
          ns = ?DEFAULT_NS :: metric_ns(),
          name             :: metric_name(),
          start_time       :: milliseconds(),
          end_time         :: milliseconds() | undefined,
          align = false    :: boolean(),
          aggregator       :: undefined | {aggregate(), precision()},
          tags             :: proplists:proplist(),
          order  = asc     :: data_order(),
          source = raw     :: data_precision()
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
        "rowkey = ? AND offset >= ? AND offset <= ? ORDER BY offset DESC LIMIT ?;">>},
     {?SELECT_STRING_INDEX,
      <<"SELECT value FROM string_index where type = ?;">>}
    ].

-spec search(options()) -> {ok, list()} | {error, any()}.
search(Options) ->
    search(?CS_READ_POOL, Options).

-spec search(pool_name(), options()) -> {ok, list()} | {error, any()}.
search(Pool, Options) when is_list(Options) ->
    case verify_options(Options, #search{}) of
        {ok, Search} ->
            do_search(Pool, Search);
        {error, _} = Error ->
            Error
    end.

-spec metric_names(metric_ns()) ->
    {ok, list(metric_ns())}
    | {error, any()}.
metric_names(Ns) ->
    metric_names(?CS_READ_POOL, Ns).

-spec metric_names(pool_name(), metric_ns()) ->
    {ok, list(metric_name())}
    | {error, any()}.
metric_names(Pool, Ns) ->
    with_pool(Pool, fun(Client) ->
                            erlcql_client:execute(Client, ?SELECT_STRING_INDEX,
                                                  [?ROW_KEY_METRIC_NAMES(Ns)], [read_consistency()])
                    end).

-spec namespaces() -> {ok, list()} | {error, any()}.
namespaces() ->
    namespaces(?CS_READ_POOL).

-spec namespaces(pool_name()) -> {ok, list()} | {error, any()}.
namespaces(Pool) ->
    case with_pool(Pool, fun(Client) ->
                            erlcql_client:execute(Client, ?SELECT_STRING_INDEX,
                                                  [?ROW_KEY_NAMESPACES], [read_consistency()])
                    end) of
        {ok, _}=Ok ->
            Ok;
        {error, not_found} ->
            {ok, []};
        {error, _}=Err ->
            Err
    end.

%%%===================================================================
%%% Internal
%%%===================================================================
-spec verify_options(options(), search()) -> {ok, search()} | {error, any()}.
verify_options([], Search) ->
    {ok, Search};
verify_options([{ns, Ns}|Options], Search) when is_binary(Ns) ->
    verify_options(Options, Search#search{ns=Ns});
verify_options([{name, Name}|Options], Search) when is_binary(Name) ->
    verify_options(Options, Search#search{name=Name});
verify_options([{start_time, Start}|Options], Search) when is_integer(Start) ->
    verify_options(Options, Search#search{start_time=Start});
verify_options([{end_time, End}|Options], Search) when is_integer(End) ->
    verify_options(Options, Search#search{end_time=End});
verify_options([{end_time, undefined}|Options], Search) ->
    verify_options(Options, Search);
verify_options([{align, Bool}|Options], Search) when is_boolean(Bool) ->
    verify_options(Options, Search#search{align=Bool});
verify_options([{aggregate, {Fun, {Val, Type}}}|Options], Search) when is_integer(Val) ->
    case lists:member(Fun, ?AGGREGATES) of
        true ->
            case lists:member(Type, ?UNITS) of
                true ->
                    verify_options(Options, Search#search{aggregator={Fun, {Val, Type}}});
                false ->
                    {error, bad_precision}
            end;
        false ->
            {error, bad_aggregate}
    end;
verify_options([{tags, List}|Options], Search) when is_list(List) ->
    verify_options(Options, Search#search{tags=List});
verify_options([{order, Order}|Options], Search) when Order =:= asc orelse Order =:= desc ->
    verify_options(Options, Search#search{order=Order});
verify_options([{source, raw}|Options], Search) ->
    verify_options(Options, Search#search{source=raw});
verify_options([{source, {Fun, {Val, Type}}}|Options], Search) when is_integer(Val) ->
    case lists:member(Fun, ?AGGREGATES) of
        true ->
            case lists:member(Type, ?UNITS) of
                true ->
                    verify_options(Options, Search#search{source={Fun, {Val, Type}}});
                false ->
                    {error, bad_precision}
            end;
        false ->
            {error, bad_aggregate}
    end;
verify_options([{source, {Fun, Val}}|Options], Search) when is_integer(Val) ->
    case lists:member(Fun, ?AGGREGATES) of
        true ->
            verify_options(Options, Search#search{source={Fun, Val}});
        false ->
            {error, bad_aggregate}
    end;
verify_options([Opt|_], _Search) ->
    {error, {bad_option, Opt}}.

read_consistency() ->
    A = freya:get_env(cassandra_read_consistency, quorum),
    {consistency, A}.

%% @doc Performs all steps of reading data from cassandra
do_search(_Pool, #search{name=undefined}) ->
    {error, {missing_param, name}};
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
        fun(Rows) -> search_data_points(Pool, S, Rows) end,
        fun(Rows) -> maybe_aggregate_data(S, Rows) end
    ],
    hope_result:pipe(Fns, undefined).

%% @doc Perform erlcql routine on a checked out pool resource
with_pool(Pool, F) when is_function(F, 1) ->
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(Pool),
    Client = erlcql_cluster_worker:get_client(Worker),
    Result = F(Client),
    erlcql_cluster:checkin(Resource),
    case Result of
        {ok, {[], _}} -> {error, not_found};
        {ok, {RowKeys, _}} -> {ok, lists:flatten(RowKeys)};
        {error, _} = Error -> Error
    end.

%% @doc Return rowkeys indexes in row_key_index
search_rows(Pool, Search) ->
    with_pool(Pool, fun(Client) -> do_search_rows(Client, Search) end).

%% @doc Return query execute result
do_search_rows(Client, #search{ns=Ns, name=Name, order=Order, source=Source,
                               start_time=StartTs, end_time=undefined}) ->
    MetricIdx        = freya_blobs:encode_idx({Ns, Name}),
    RowWidth         = freya_utils:row_width(Source),
    StartRowTime     = freya_utils:floor(StartTs, RowWidth),
    {ok, StartTsBin} = freya_blobs:encode_search_key({Ns, Name}, StartRowTime, raw),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_FROM_START(Order),
                          [MetricIdx, StartTsBin],
                          [read_consistency()]);
do_search_rows(Client, #search{ns=Ns, name=Name, order=Order, source=Source,
                               start_time=StartTs, end_time=EndTs}) ->
    MetricIdx    = freya_blobs:encode_idx({Ns, Name}),
    RowWidth     = freya_utils:row_width(Source),
    StartRowTime = freya_utils:floor(StartTs, RowWidth),
    EndRowTime   = freya_utils:floor(EndTs, RowWidth) + 1,
    {ok, StartTsBin} = freya_blobs:encode_search_key(Name, StartRowTime, raw),
    {ok, EndTsBin}   = freya_blobs:encode_search_key(Name, EndRowTime, raw),
    erlcql_client:execute(Client,
                          ?SELECT_ROWS_IN_RANGE(Order),
                          [MetricIdx, StartTsBin, EndTsBin],
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
    fun({Key, Values}) when FilterKey =:= Key ->
            lists:member(FilterValue, Values);
       (_) -> false
    end.

%% @doc Return a function to match at row tags
matching_tags(RowTags) ->
    fun(MatchFun) ->
        lists:any(MatchFun, RowTags)
    end.

%% @doc Return result of querying each rowkey, done in parallel using pmap
search_data_points(Pool, #search{}=S, Rows) ->
    Results0 = freya_utils:pmap(fun do_search_data_points/3, [Pool, S], Rows),
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
                          order=Order, source=Source}=S, Acc) ->
    ReadRowSize     = ?MODULE:read_row_size(),
    StartOffsetBin  = start_time_bin(StartTime, RowProps),
    EndOffsetBin    = end_time_bin(EndTime, RowProps, Source),
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
end_time_bin(EndTime, RowProps, Source) ->
    RowTime    = proplists:get_value(row_time, RowProps),
    RowWidth   = freya_utils:row_width(Source),
    RowWidthMs = freya_utils:ms(RowWidth),
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

%% @doc Helper record to fold over a list of data points
-record(aggr, {
          epoch             :: any(),
          epoch_fun         :: fun(),
          epoch_data        :: dict(),
          aggregator_funs   :: {fun(), fun()},
          timeline = []     :: [data_point()]
         }).

%% @doc Aggregates data for given aggregator
maybe_aggregate_data(#search{aggregator=undefined}, Rows) ->
    {ok, Rows};
maybe_aggregate_data(#search{}, []) ->
    {ok, []};
maybe_aggregate_data(#search{aggregator={Fun, _}}=S, Rows) ->
    Aggr = #aggr{epoch_fun=epoch_fun(S),
                 aggregator_funs=freya_utils:aggregator_funs(Fun)},
    Result0 = lists:foldl(fold_data_points_fun(S), Aggr, Rows),
    #aggr{timeline=DataPoints} = emit(Result0),
    {ok, lists:reverse(DataPoints)}.

%% @doc Return a function to calculate a base of time interval
epoch_fun(#search{start_time=StartTime, aggregator={_,Precision}}) ->
    Range = freya_utils:ms(Precision),
    fun(Ts) ->
            ((Ts - StartTime) div Range) * Range + StartTime
    end.

%% @doc Return a function to fold over list of data points
fold_data_points_fun(#search{}=S) ->
    AggregateFun = aggregate_fun(S),
    fun(#data_point{}=DP, #aggr{}=Acc0) ->
        Acc = maybe_emit(DP, Acc0),
        AggregateFun(DP, Acc)
    end.

%% @doc Return a function that aggregate given data point
aggregate_fun(#search{align=Align}) ->
    AggregatorStartFun  = aggregator_start_fun(Align),
    fun(#data_point{value=V1}=DP0, #aggr{epoch_data=Data0,
                                         aggregator_funs={Accumulate, _}}=Acc) ->
        Key = aggregator_key(DP0, Acc),
        DP = case dict:find(Key, Data0) of
                 error ->
                     DP0#data_point{ts=AggregatorStartFun(DP0, Acc),
                                    value=Accumulate(V1, undefined)};
                 {ok, #data_point{value=V2}=DP1} ->
                     DP1#data_point{value=Accumulate(V1, V2)}
             end,
        Acc#aggr{epoch_data=dict:store(Key, DP, Data0)}
    end.

%% @doc Return a function that returns a timestamp of interval
aggregator_start_fun(false) ->
    fun(#data_point{ts=Ts}, _Acc) -> Ts end;
aggregator_start_fun(true) ->
    fun(#data_point{ts=Ts}, #aggr{epoch_fun=EpochFun}) -> EpochFun(Ts) end.

%% @doc Generate a key for grouping values in one interval,
%% at the moment we support only time grouping
aggregator_key(#data_point{ts=Ts, type=Type}, #aggr{epoch_fun=EpochFun}) ->
    [{ts, EpochFun(Ts)}, {type, Type}].

%% @doc Check if an interval is finished and if so, emits aggregated data
maybe_emit(#data_point{ts=Ts}, #aggr{epoch=undefined, epoch_fun=EpochFun}=Acc0) ->
    Acc0#aggr{epoch=EpochFun(Ts), epoch_data=dict:new()};
maybe_emit(#data_point{ts=Ts}, #aggr{epoch=Epoch, epoch_fun=EpochFun}=Acc0) ->
    case EpochFun(Ts) > Epoch of
        true ->
            Acc = emit(Acc0),
            Acc#aggr{epoch_data=dict:new(), epoch=EpochFun(Ts)};
        false ->
            Acc0
    end.

%% @doc Emits aggregated data from current interval to timeline
emit(#aggr{epoch_data=D, timeline=DataPoints, aggregator_funs={_, Emit}}=A) ->
    EpochDataPoints0 = [DP || {_, DP} <- dict:to_list(D)],
    EpochDataPoints  = lists:map(
                         fun(#data_point{value=V}=DP) ->
                            DP#data_point{value=Emit(V)}
                         end, EpochDataPoints0),
    A#aggr{timeline=EpochDataPoints ++ DataPoints}.
