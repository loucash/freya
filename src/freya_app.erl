-module(freya_app).
-behaviour(application).

-include("freya.hrl").

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================
start(_StartType, _StartArgs) ->
    {ok, Publisher} = eqm:start_publisher(?CS_WRITERS_PUB),
    ok = start_cassandra_cluster(),
    ok = start_cass_writers_pool(Publisher),
    freya_sup:start_link().

stop(_State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_cassandra_cluster() ->
    {ok, CassandraPools} = freya:get_env(cassandra_pools),
    start_cassandra_pools(CassandraPools).

start_cassandra_pools([]) -> ok;
start_cassandra_pools([{Name, Options0}|Pools]) ->
    {ok, CassandraKeyspace} = freya:get_env(cassandra_keyspace),
    Options = [{use, CassandraKeyspace},
               {prepare, freya_cass:statements()}|Options0],
    ok = erlcql_cluster:new(Name, Options),
    start_cassandra_pools(Pools).

start_cass_writers_pool(Publisher) ->
    {ok, WritersCount} = freya:get_env(writers_size),
    PoolOptions = [{name, {local, ?CS_WRITERS_POOL}},
                   {size, WritersCount},{max_overflow, 0},
                   {worker_module, freya_writer}],
    {ok, _} = poolboy:start_link(PoolOptions, Publisher),
    ok.
