-module(freya_http_SUITE).
-compile([export_all]).

-define(th, test_helpers).

suite() ->
    [{timetrap, {seconds, 40}}].

all() -> [t_list_namespaces,
          t_list_metricnames,
          t_query_metrics_start_time,
          t_query_metrics_with_tags,
          t_query_aggregate].

init_per_suite(Config) ->
    ?th:setup_env(),
    freya:start(),
    frik:start(),
    Config2 = ?th:set_fixt_dir(?MODULE, Config),
    Config2.

end_per_suite(_Config) ->
    ok = frik:stop(),
    ok = freya:stop().

sample_metrics(BaseName, Setup) ->
    sample_metrics(BaseName, Setup, fun(_N) -> random:uniform() end).

sample_metrics(BaseName, {HowManyMetrics,HowManyDps}, SampleFun) ->
    [Ns|Names] = [?th:randomize(BaseName) || _ <- lists:seq(0,HowManyMetrics)],
    Ts = tic:now_to_epoch_msecs(),
    Metrics = [
               [[Ts-100*N, SampleFun(N)] || N <- lists:seq(HowManyDps,1,-1)]
               || _ <- Names
              ],
    {Ns, lists:zip(Names, Metrics)}.

t_list_namespaces(_) ->
    {Ns, Metrics} = sample_metrics(<<"t_list_namespaces">>, {2,10}),
    ok = frik:put_metrics(Ns, Metrics),
    Assert = fun() ->
                     {ok,Nss} = frik:list_namespaces(),
                     lists:member(Ns,Nss) 
             end,
    ?th:keep_trying(true, Assert, 100, 50),
    ok.

t_list_metricnames(_) ->
    {Ns, [{N1,_}]=Metrics} = sample_metrics(<<"t_list_metricnames">>, {1,20}),
    ok = frik:put_metrics(Ns, Metrics),
    Assert = fun() ->
                     {ok,Names} = frik:list_metric_names(Ns),
                     lists:member(N1,Names) 
             end,
    ?th:keep_trying(true, Assert, 100, 50),
    ok.

t_query_metrics_start_time(_) ->
    {Ns, [{N1,DPs}]=Metrics} = sample_metrics(<<"t_query_metrics_start_time">>, {1,13}),
    ok = frik:put_metrics(Ns, Metrics),
    StartTime = tic:now_to_epoch_msecs() - timer:seconds(10),
    Assert = fun() -> frik:query_metrics(Ns,N1,[{start_time, StartTime}]) end,
    ?th:keep_trying({ok,DPs}, Assert, 100, 50),
    ok.

t_query_metrics_with_tags(_) ->
    {Ns, [{N1,DPs1},{_,DPs2}]} = sample_metrics(<<"t_query_metrics_tags">>, {2,3}),
    Tag1 = {<<"level">>,<<":og;,">>},
    Tag2 = {<<"level">>,<<"b:os,s">>},
    Tag3 = {<<"street_cred">>,<<"!@#$%^&*()">>},
    Tag4 = {<<"m:a:c,">>,<<"de:ad:be:ef:,">>},
    ok = frik:put_metrics(Ns, [{N1,DPs1}], [Tag1,Tag3,Tag4]),
    ok = frik:put_metrics(Ns, [{N1,DPs2}], [Tag2,Tag3,Tag4]),
    StartTime = tic:now_to_epoch_msecs() - timer:seconds(10),
    Assert = fun() ->
                     Opts = [{start_time,StartTime},{tags,[Tag2,Tag4]}],
                     frik:query_metrics(Ns,N1,Opts)
             end,
    ?th:keep_trying({ok,DPs2}, Assert, 100, 50),
    ok.

t_query_aggregate(_) ->
    Sample = fun(N) when N rem 2 == 0 -> 10;
                (_) -> 1
             end,
    {Ns, [{N1,DPs1}]} = sample_metrics(<<"t_query_aggregate">>, {1,10}, Sample),
    ok = frik:put_metrics(Ns, [{N1,DPs1}]),
    StartTime = tic:now_to_epoch_msecs() - timer:seconds(10),
    Assert = fun() ->
                     Opts = [{start_time,StartTime},
                             {aggregate,sum},
                             {sampling,{1,hours}}],
                     {ok, [[_,Sum]]} = frik:query_metrics(Ns,N1,Opts),
                     {sum, Sum}
             end,
    ?th:keep_trying({sum,55}, Assert, 100, 50),
    ok.
