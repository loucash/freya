%%%-------------------------------------------------------------------
%%% @doc
%%% Worker responsible for batch insert of data points into cassandra
%%% @end
%%%-------------------------------------------------------------------
-module(freya_writer).
-behaviour(gen_server).

-include("freya.hrl").
-include("freya_metrics.hrl").
-include("freya_writer.hrl").

%% API
-export([start_link/1]).
-export([statements/0]).
-export([save/2, save/3]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          write_delay,
          subscriber,
          timer
         }).

-type save_options() :: [{ttl, ttl()} | {aggregate, data_precision()}].

%%%===================================================================
%%% API
%%%===================================================================
statements() ->
    [
     {?INSERT_DATA_POINT_TTL,
      <<"INSERT INTO data_points (rowkey, offset, value) "
        "VALUES (?, ?, ?) USING TTL ?;">>},
     {?INSERT_DATA_POINT,
      <<"INSERT INTO data_points (rowkey, offset, value) "
        "VALUES (?, ?, ?);">>},
     {?INSERT_ROW_INDEX,
      <<"INSERT INTO row_key_index (metric_name, rowkey, unused) "
        "VALUES (?, ?, ?);">>},
     {?INSERT_ROW_INDEX_TTL,
      <<"INSERT INTO row_key_index (metric_name, rowkey, unused) "
        "VALUES (?, ?, ?) USING TTL ?;">>},
     {?INSERT_STRING_INDEX,
      <<"INSERT INTO string_index (type, value, unused) "
        "VALUES (?, ?, ?);">>}
    ].

-spec save(eqm:pub(), data_point()) -> ok | {error, no_capacity}.
save(Publisher, DP) ->
    TTL = freya:get_env(raw_ttl, infinity),
    save(Publisher, DP, [{aggregate, raw}, {ttl, TTL}]).

-spec save(eqm:pub(), data_point(), save_options()) -> ok | {error, no_capacity}.
save(Publisher, DataPoint, Opts) ->
    T = quintana:begin_timed(?Q_WRITER_BLOB),
    TTL = proplists:get_value(ttl, Opts, infinity),
    DataPrecision = proplists:get_value(aggregate, Opts, raw),
    Qrys = save_data_point_queries(DataPoint, TTL, DataPrecision),
    R = eqm_pub:post(Publisher, Qrys),
    quintana:notify_timed(T),
    R.

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Publisher) ->
    {ok, BatchSize} = freya:get_env(write_batch_size),
    {ok, WriteDelay} = freya:get_env(write_delay),
    {ok, Subscriber} = eqm:start_subscriber(Publisher, BatchSize, notify_full),
    Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
    {ok, #state{subscriber=Subscriber, write_delay=WriteDelay, timer=Timer}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({mail, _, Queries0, _}, #state{write_delay=WriteDelay,
                                           subscriber=Subscriber}=State) ->
    T = quintana:begin_timed(?Q_WRITER_BATCH),
    Queries = lists:flatten(Queries0),
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(?CS_WRITE_POOL),
    Client = erlcql_cluster_worker:get_client(Worker),
    Res = erlcql_client:batch(Client, Queries, [consistency()]),
    L = length(Queries0),
    case Res of
        {ok, _} ->
            freya_tcp_status:inc(metrics_saved, L),
            ok;
        {error, Reason} ->
            lager:error("Error during batch save: ~p", [Reason]),
            error
    end,
    erlcql_cluster:checkin(Resource),
    Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
    eqm_sub:notify_full(Subscriber),
    quintana:notify_timed(T),
    {noreply, State#state{timer=Timer}};
handle_info({mail, _, buffer_full}, #state{timer=TimerRef,
                                           subscriber=Subscriber}=State) ->
    erlang:cancel_timer(TimerRef),
    eqm_sub:active(Subscriber),
    {noreply, State};
handle_info(flush_buffer_timeout, #state{write_delay=WriteDelay,
                                         subscriber=Subscriber}=State) ->
    case eqm_sub:info(Subscriber) of
        {ok, [_, {size, 0}]} ->
            Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
            {noreply, State#state{timer=Timer}};
        {ok, _} ->
            eqm_sub:active(Subscriber),
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
consistency() ->
    A = freya:get_env(cassandra_write_consistency, quorum),
    {consistency, A}.

-spec save_data_point_queries(data_point(), ttl(), data_precision()) -> list().
save_data_point_queries(DP, TTL, DataPrecision) ->
    Name = freya_data_point:name(DP),
    Tags = freya_data_point:tags(DP),
    {ok, {RowKey, Offset, Value}} = freya_data_point:encode(DP, DataPrecision),
    [insert_data_point(RowKey, Offset, Value, TTL),
     insert_row_index(Name, RowKey, TTL, DataPrecision),
     insert_string_index(?ROW_KEY_METRIC_NAMES, Name)]
    ++ lists:flatmap(
         fun({TagName, TagValues}) ->
            [insert_string_index(?ROW_KEY_TAG_NAMES, TagName)]
            ++ lists:map(
                 fun(TagValue) ->
                    insert_string_index(?ROW_KEY_TAG_VALUES, TagValue)
                 end, TagValues)
         end, Tags).

insert_data_point(RowKey, Offset, Value, infinity) ->
    {?INSERT_DATA_POINT, [RowKey, Offset, Value]};
insert_data_point(RowKey, Offset, Value, TTL) when is_integer(TTL) ->
    {?INSERT_DATA_POINT_TTL, [RowKey, Offset, Value, TTL]}.

insert_row_index(Name, RowKey, infinity, _DataPrecision) ->
    {?INSERT_ROW_INDEX, [Name, RowKey, <<0>>]};
insert_row_index(Name, RowKey, TTL, DataPrecision) ->
    RowIndexTTL = row_index_ttl(TTL, DataPrecision),
    {?INSERT_ROW_INDEX_TTL, [Name, RowKey, <<0>>, RowIndexTTL]}.

row_index_ttl(TTL, DataPrecision) ->
    RowWidth    = freya_utils:row_width(DataPrecision),
    RowWidthMs  = freya_utils:ms(RowWidth),
    RowWidthSeconds = RowWidthMs div 1000,
    lists:min([?MAX_TTL, RowWidthSeconds + TTL]).

insert_string_index(Key, Value) ->
    {?INSERT_STRING_INDEX, [Key, Value, <<0>>]}.
