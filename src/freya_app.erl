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
    ok = freya_rest:start(),
    ok = start_frontend(),
    case freya_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, freya_stats_vnode}]),
            ok = riak_core_node_watcher:service_up(freya_stats, self()),
            {ok, Pid};
        {error, _} = Error ->
            Error
    end.

stop(_State) ->
    ok.

start_cassandra_cluster() ->
    {ok, CassandraPools} = freya:get_env(cassandra_pools),
    start_cassandra_pools(CassandraPools).

start_cassandra_pools([]) -> ok;
start_cassandra_pools([{Name, Options0}|Pools]) ->
    {ok, CassandraKeyspace} = freya:get_env(cassandra_keyspace),
    Statements = freya_writer:statements() ++
                 freya_reader:statements(),
    Options = [{use, CassandraKeyspace},
               {prepare, Statements}|Options0],
    ok = erlcql_cluster:new(Name, Options),
    start_cassandra_pools(Pools).

start_cass_writers_pool(Publisher) ->
    {ok, WritersCount} = freya:get_env(writes_workers_pool_size),
    PoolOptions = [{name, {local, ?CS_WRITERS_POOL}},
                   {size, WritersCount},{max_overflow, 0},
                   {worker_module, freya_writer}],
    {ok, _} = poolboy:start_link(PoolOptions, Publisher),
    ok.

start_frontend() ->
    FrontendPort = freya:get_env(frontend_port, 8080),
    Dispatch = cowboy_router:compile([
                                      {'_', [ {"/[...]", cowboy_static,
                                              {priv_dir, freya, "frontend",
                                               [{mimetypes, cow_mimetypes, all}]}} ]}
                                     ]),
    {ok, _} = cowboy:start_http(frontend, 100, [{port, FrontendPort}], [ {env, [{dispatch, Dispatch}]} ]),
    ok.
