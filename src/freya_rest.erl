-module(freya_rest).

-export([start/0]).

-define(DEFAULT_PORT, 8080).

start() ->
    MetricsNames = {"/api/v1/metricnames", freya_rest_metrics, []},
    MetricsQuery = {"/api/v1/datapoints/query", freya_rest_dps, []},
    UI = {"/[...]", cowboy_static, {priv_dir, freya, "ui", [{mimetypes, cow_mimetypes, all}]}},
    Dispatch = cowboy_router:compile([ {'_', [MetricsNames,
                                              MetricsQuery,
                                              UI]
                                       } ]),
    Port = freya:get_env(rest_port, ?DEFAULT_PORT),
    {ok, _} = cowboy:start_http(http, 100,
                                [{port, Port}], [ {env, [{dispatch, Dispatch}]}
                                                ]),
    ok.
