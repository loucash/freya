-module(freya_io_SUITE).

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([t_write_read_data_point/1,
         t_write_read_different_rows/1,
         t_read_row_size/1,
         t_read_row_size_and_desc/1,
         t_filter_tags_1/1,
         t_filter_tags_2/1,
         t_sum_aggregate/1,
         t_sum_aggregate_aligned/1,
         t_sum_aggregate_aligned_different_types/1,
         t_avg_aggregate/1,
         t_avg_aggregate_aligned/1,
         t_start_end_time/1,
         t_start_end_time_different_rowkeys/1,
         t_tcp_version/1,
         t_tcp_write/1]).

-include("freya.hrl").
-include_lib("common_test/include/ct.hrl").

-define(th, test_helpers).

suite() ->
    [{timetrap, {seconds, 40}}].

all() ->
    [
     t_write_read_data_point,
     t_write_read_different_rows,
     t_read_row_size,
     t_read_row_size_and_desc,
     t_start_end_time,
     t_start_end_time_different_rowkeys,
     t_filter_tags_1,
     t_filter_tags_2,
     t_sum_aggregate,
     t_sum_aggregate_aligned,
     t_sum_aggregate_aligned_different_types,
     t_avg_aggregate,
     t_avg_aggregate_aligned,
     t_tcp_version,
     t_tcp_write
    ].

init_per_suite(Config) ->
    freya:start(),
    Config.

end_per_suite(_Config) ->
    freya:stop(),
    ok.

t_write_read_data_point(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"test_write_read_data_point">>),
    Ts = tic:now_to_epoch_msecs(),
    DPIn = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn),
    ?th:keep_trying({ok, [DPIn]}, fun() ->
                                          freya_reader:search(?CS_READ_POOL,
                                                              [{metric_name, MetricName},
                                                               {start_time, Ts}])
                                  end, 100, 200).

t_write_read_different_rows(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"test_write_read_different_rows">>),
    Ts1 = tic:now_to_epoch_msecs(),
    Ts2 = tic:now_to_epoch_msecs() + freya_utils:ms(?ROW_WIDTH)+1,
    DPIn1 = freya_data_point:new(MetricName, Ts1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts2, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ?th:keep_trying({ok, [DPIn1, DPIn2]}, fun() ->freya_reader:search(?CS_READ_POOL,
                                               [{metric_name, MetricName},
                                                {start_time, Ts1}])
                                          end, 100, 200).

t_read_row_size(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"test_read_row_size">>),
    Ts = tic:now_to_epoch_msecs(),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+3, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+4, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    ?th:keep_trying({ok, [DPIn1, DPIn2, DPIn3, DPIn4]},
                    fun() ->
                            freya_reader:search(?CS_READ_POOL,
                                                [{metric_name, MetricName},
                                                 {start_time, Ts}])
                    end, 100, 200),
    meck:unload().

t_read_row_size_and_desc(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"test_read_row_size">>),
    Ts = tic:now_to_epoch_msecs(),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+3, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+4, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    ?th:keep_trying({ok, [DPIn4, DPIn3, DPIn2, DPIn1]},
                    fun() -> freya_reader:search(?CS_READ_POOL,
                                                 [{metric_name, MetricName},
                                                  {start_time, Ts},
                                                  {order, desc}])
                    end, 100, 200),
    meck:unload(),
    ok.

t_start_end_time(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"test_start_end_time">>),
    Ts = tic:now_to_epoch_msecs(),
    DPIn1 = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+3, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    ?th:keep_trying({ok, [DPIn1, DPIn2]},
                    fun() ->freya_reader:search(?CS_READ_POOL,
                                                [{metric_name, MetricName},
                                                 {start_time, Ts},
                                                 {end_time, Ts+1}])
                    end, 100, 200).

t_start_end_time_different_rowkeys(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"test_start_end_time">>),
    Ts1 = freya_utils:floor(tic:now_to_epoch_msecs(), ?ROW_WIDTH),
    Ts2 = freya_utils:ceil(tic:now_to_epoch_msecs(), ?ROW_WIDTH),
    DPIn1 = freya_data_point:new(MetricName, Ts1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts2, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ?th:keep_trying({ok, [DPIn1, DPIn2]},
                    fun() -> freya_reader:search(?CS_READ_POOL,
                                                 [{metric_name, MetricName},
                                                  {start_time, Ts1},
                                                  {end_time, Ts2}])
                    end, 100, 200).

t_filter_tags_1(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName = ?th:randomize(<<"test_filter_tags_1">>),
    Ts = tic:now_to_epoch_msecs(),
    DPIn1 = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1,
                                 [{<<"foo">>, <<"bar">>}]),
    DPIn2 = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1,
                                 [{<<"baz">>, <<"fox">>}]),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ?th:keep_trying({ok, [DPIn1]}, fun() ->freya_reader:search(?CS_READ_POOL,
                                        [{metric_name, MetricName},
                                         {start_time, Ts},
                                         {tags, [{<<"foo">>, <<"bar">>}]}])
                                   end, 100, 200).

t_filter_tags_2(_Config) ->
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName = ?th:randomize(<<"test_filter_tags_2">>),
    Ts = tic:now_to_epoch_msecs(),
    DPIn = freya_data_point:new(MetricName, Ts, <<"kairos_long">>, 1,
                                [{<<"foo">>, <<"bar">>},
                                 {<<"baz">>, <<"fox">>}]),
    ok = freya_writer:save(Publisher, DPIn),
    ?th:keep_trying({ok, [DPIn]},
                    fun() -> freya_reader:search(?CS_READ_POOL,
                                                 [{metric_name, MetricName},
                                                  {start_time, Ts},
                                                  {tags, [{<<"foo">>, <<"bar">>}]}])
                    end, 100, 200).

t_sum_aggregate(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"t_sum_aggregate">>),
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), {1, seconds}),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+3, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+4, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    DPOut = DPIn1#data_point{value=4},
    ?th:keep_trying({ok, [DPOut]},
                    fun() ->
                            freya_reader:search(?CS_READ_POOL,
                                                [{metric_name, MetricName},
                                                 {start_time, Ts},
                                                 {aggregate, {sum, {1, seconds}}}])
                    end, 100, 200),
    meck:unload().

t_sum_aggregate_aligned(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"t_sum_aggregate_aligned">>),
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), {1, seconds}),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+3, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+4, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    DPOut = DPIn1#data_point{value=4, ts=Ts},
    ?th:keep_trying({ok, [DPOut]},
                    fun() ->
                            freya_reader:search(?CS_READ_POOL,
                                                [{metric_name, MetricName},
                                                 {start_time, Ts},
                                                 {aggregate, {sum, {1, seconds}}},
                                                 {align, true}])
                    end, 100, 200),
    meck:unload().

t_sum_aggregate_aligned_different_types(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"t_sum_aggregate_aligned_different_types">>),
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), {1, seconds}),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+3, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+4, <<"kairos_long">>, 1),
    DPIn5 = freya_data_point:new(MetricName, Ts+1, <<"kairos_double">>, 1.0),
    DPIn6 = freya_data_point:new(MetricName, Ts+2, <<"kairos_double">>, 1.0),
    DPIn7 = freya_data_point:new(MetricName, Ts+3, <<"kairos_double">>, 1.0),
    DPIn8 = freya_data_point:new(MetricName, Ts+4, <<"kairos_double">>, 1.0),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    ok = freya_writer:save(Publisher, DPIn5),
    ok = freya_writer:save(Publisher, DPIn6),
    ok = freya_writer:save(Publisher, DPIn7),
    ok = freya_writer:save(Publisher, DPIn8),
    DPOut1 = DPIn1#data_point{value=4, ts=Ts},
    DPOut2 = DPIn5#data_point{value=4.0, ts=Ts},
    Set = sets:from_list([DPOut1, DPOut2]),
    ?th:keep_trying(Set,
                    fun() ->
                            {ok, L} = freya_reader:search(?CS_READ_POOL,
                                                          [{metric_name, MetricName},
                                                           {start_time, Ts},
                                                           {aggregate, {sum, {1, seconds}}},
                                                           {align,
                                                            true}]),
                            sets:from_list(L)
                    end, 100, 300),
    meck:unload().

t_avg_aggregate(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"t_avg_aggregate">>),
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), {1, seconds}),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+1001, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+1002, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    DPOut1 = DPIn1#data_point{value=1.0},
    DPOut2 = DPIn3#data_point{value=1.0},
    ?th:keep_trying({ok, [DPOut1, DPOut2]},
                    fun() ->
                            freya_reader:search(?CS_READ_POOL,
                                                [{metric_name, MetricName},
                                                 {start_time, Ts},
                                                 {aggregate, {avg, {1, seconds}}}])
                    end, 100, 200),
    meck:unload().

t_avg_aggregate_aligned(_Config) ->
    meck:new(freya_reader, [passthrough]),
    meck:expect(freya_reader, read_row_size, fun() -> 2 end),
    {ok, Publisher} = eqm:publisher_info(?CS_WRITERS_PUB),
    MetricName  = ?th:randomize(<<"t_avg_aggregate_aligned">>),
    Ts = freya_utils:floor(tic:now_to_epoch_msecs(), {1, seconds}),
    DPIn1 = freya_data_point:new(MetricName, Ts+1, <<"kairos_long">>, 1),
    DPIn2 = freya_data_point:new(MetricName, Ts+2, <<"kairos_long">>, 1),
    DPIn3 = freya_data_point:new(MetricName, Ts+1001, <<"kairos_long">>, 1),
    DPIn4 = freya_data_point:new(MetricName, Ts+1002, <<"kairos_long">>, 1),
    ok = freya_writer:save(Publisher, DPIn1),
    ok = freya_writer:save(Publisher, DPIn2),
    ok = freya_writer:save(Publisher, DPIn3),
    ok = freya_writer:save(Publisher, DPIn4),
    DPOut1 = DPIn1#data_point{ts=Ts, value=1.0},
    DPOut2 = DPIn3#data_point{ts=Ts+1000, value=1.0},
    ?th:keep_trying({ok, [DPOut1, DPOut2]},
                    fun() ->
                            freya_reader:search(?CS_READ_POOL,
                                                [{metric_name, MetricName},
                                                 {start_time, Ts},
                                                 {aggregate, {avg, {1, seconds}}},
                                                 {align, true}])
                    end, 100, 200),
    meck:unload().

t_tcp_version(_Config) ->
    {ok, Client} = freya_tcp_client:start_link(),
    {ok, Vsn} = freya:version(),
    {ok, [[{<<"version">>, Vsn}]]} = freya_tcp_client:version(Client),
    {ok, [[{<<"version">>, Vsn}]]} = freya_tcp_client:version(Client),
    ok = freya_tcp_client:stop(Client).

t_tcp_write(_Config) ->
    {ok, Client} = freya_tcp_client:start_link(),
    MetricName = ?th:randomize(<<"via_tcp">>),
    Ts = tic:now_to_epoch_msecs(),
    ok = freya_tcp_client:put_metric(Client, MetricName, Ts, 666.66),
    DPIn = freya_data_point:new(MetricName, Ts, <<"kairos_double">>, 666.66),
    ?th:keep_trying({ok, [DPIn]}, fun() -> freya_reader:search(?CS_READ_POOL,
                                                 [{metric_name, MetricName},
                                                  {start_time, Ts}])
                    end, 100, 200).
