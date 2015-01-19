-module(freya_rest).

-export([start/0]).

start() ->
    MetricsNames = {"/api/v1/metricnames", freya_rest_metrics, []},
    MetricsQuery = {"/api/v1/datapoints/query", freya_rest_dps, []},
    UI = {"/[...]", cowboy_static, {priv_dir, freya, "ui", [{mimetypes, cow_mimetypes, all}]}},
    Dispatch = cowboy_router:compile([ {'_', [MetricsNames,
                                              MetricsQuery,
                                              UI]
                                       } ]),
    {ok, _} = cowboy:start_http(http, 100,
                                [{port, 8080}], [ {env, [{dispatch, Dispatch}]}
                                                ]),
    ok.
