-module(freya_app).
-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================
start(_StartType, _StartArgs) ->
    ok = start_cassandra_cluster(),
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
               {prepare, []}|Options0],
    ok = erlcql_cluster:new(Name, Options),
    start_cassandra_pools(Pools).
