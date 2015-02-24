-module(freya_rest).

-export([start/0]).

-define(DEFAULT_PORT, freya:get_env(http_port, 8666)).
-define(PFX, "/api/v1").

start() ->
    Routes = [
              {?PFX++"/metrics/ns", freya_rest_ns, []},
              {?PFX++"/metrics/ns/:ns", freya_rest_names, []},
              {?PFX++"/metrics/ns/:ns/:metric_name", freya_rest_dps, []},

              {?PFX++"/health", freya_rest_health, []},
              {?PFX++"/admin/node/:cmd", freya_rest_admin_node, []}
             ],

    Dispatch = cowboy_router:compile([ {'_', Routes} ]),
    Port = freya:get_env(rest_port, ?DEFAULT_PORT),
    {ok, _} = cowboy:start_http(http, 100,
                                [{port, Port}], [ {env, [{dispatch, Dispatch}]}
                                                ]),
    ok.
