%%%-------------------------------------------------------------------
%%% @doc
%%% Worker responsible for batch insert of data points into cassandra
%%% @end
%%%-------------------------------------------------------------------
-module(freya_writer).
-behaviour(gen_server).

-include("freya.hrl").

%% API
-export([start_link/1]).
-export([statements/0]).
-export([save/2]).

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
     {?INSERT_STRING_INDEX,
      <<"INSERT INTO string_index (type, value, unused) "
        "VALUES (?, ?, ?);">>}
    ].

-spec save(eqm:pub(), data_point()) -> ok | {error, no_capacity}.
save(Publisher, #data_point{}=DP) ->
    Qrys = save_data_point_queries(DP),
    eqm_pub:post(Publisher, Qrys).

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

handle_info({mail, _, Queries0, _}, #state{write_delay=WriteDelay}=State) ->
    Queries = lists:flatten(Queries0),
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(?CS_WRITE_POOL),
    Client = erlcql_cluster_worker:get_client(Worker),
    Res = erlcql_client:batch(Client, Queries, [consistency()]),
    case Res of
        {ok, _} ->
            % log ok
            ok;
        {error, _Reason} ->
            % log error
            error
    end,
    erlcql_cluster:checkin(Resource),
    Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
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

-spec save_data_point_queries(data_point()) -> list().
save_data_point_queries(#data_point{}=DP) ->
    {ok, {Row, Column, Value}} = freya_data_point:encode(DP),
    [{?INSERT_DATA_POINT, [Row, Column, Value]},
     {?INSERT_ROW_INDEX, [DP#data_point.name, Row, <<0>>]},
     {?INSERT_STRING_INDEX, [?ROW_KEY_METRIC_NAMES, DP#data_point.name, <<0>>]}]
    ++ lists:flatmap(
         fun({TagName, TagValue}) ->
            [{?INSERT_STRING_INDEX, [?ROW_KEY_TAG_NAMES, TagName, <<0>>]},
             {?INSERT_STRING_INDEX, [?ROW_KEY_TAG_VALUES, TagValue, <<0>>]}]
         end, DP#data_point.tags).
