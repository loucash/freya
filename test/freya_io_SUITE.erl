-module(freya_io_SUITE).

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([t_write_read_data_point/1,
         t_tcp_version/1]).

-include("freya.hrl").
-include_lib("common_test/include/ct.hrl").


suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
     t_write_read_data_point,
     t_tcp_version
    ].

init_per_suite(Config) ->
    freya:start(),
    Config.

end_per_suite(_Config) ->
    freya:stop(),
    ok.

t_write_read_data_point(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName = <<"test_metric_name">>,
    Ts = 1418911482000,
    DP1 = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DP1),
    timer:sleep(2000),
    {ok, {_, Worker}=Resource} = erlcql_cluster:checkout(?CS_READ_POOL),
    Client = erlcql_cluster_worker:get_client(Worker),
    {ok, [DP2]} = freya_reader:search(Client, MetricName, Ts-1),
    erlcql_cluster:checkin(Resource),
    true = DP1 =:= DP2,
    ok.

t_tcp_version(_Config) ->
    {ok, Client} = freya_tcp_client:start_link(),
    {ok, Vsn} = freya:version(),
    {ok, Vsn} = freya_tcp_client:version(Client),
    {ok, Vsn} = freya_tcp_client:version(Client).
