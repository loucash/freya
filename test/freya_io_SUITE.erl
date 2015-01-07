-module(freya_io_SUITE).

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([t_write_read_data_point/1,
         t_filter_tags_1/1,
         t_filter_tags_2/1,
         t_tcp_version/1]).

-include("freya.hrl").
-include_lib("common_test/include/ct.hrl").


suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
     t_write_read_data_point,
     t_filter_tags_1,
     t_filter_tags_2,
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
    MetricName  = <<"test_write_read_data_point">>,
    Ts = tic:now_to_epoch_msecs(),
    DPIn = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn),
    timer:sleep(2000),
    {ok, [DPOut]} = freya_reader:search(?CS_READ_POOL, MetricName, Ts-1),
    true = DPIn =:= DPOut,
    ok.

t_filter_tags_1(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName = <<"test_filter_tags_1">>,
    Ts = tic:now_to_epoch_msecs(),
    DPIn1 = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1,
                                 [{<<"foo">>, <<"bar">>}]),
    DPIn2 = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1,
                                 [{<<"baz">>, <<"fox">>}]),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    timer:sleep(2000),
    {ok, [DPOut]} = freya_reader:search(?CS_READ_POOL, MetricName, Ts-1,
                                        [{tags, [{<<"foo">>, <<"bar">>}]}]),
    true = DPIn1 =:= DPOut,
    ok.

t_filter_tags_2(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName = <<"test_filter_tags_2">>,
    Ts = tic:now_to_epoch_msecs(),
    DPIn = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1,
                                [{<<"foo">>, <<"bar">>},
                                 {<<"baz">>, <<"fox">>}]),
    ok = freya_writer:save(Publisher, DPIn),
    timer:sleep(2000),
    {ok, [DPOut]} = freya_reader:search(?CS_READ_POOL, MetricName, Ts-1,
                                        [{tags, [{<<"foo">>, <<"bar">>}]}]),
    true = DPIn =:= DPOut,
    ok.

t_tcp_version(_Config) ->
    {ok, Client} = freya_tcp_client:start_link(),
    {ok, Vsn} = freya:version(),
    {ok, Vsn} = freya_tcp_client:version(Client),
    {ok, Vsn} = freya_tcp_client:version(Client).
