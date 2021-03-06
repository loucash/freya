-module(freya_rest).

-export([start/0]).

-define(DEFAULT_PORT, 8666).
-define(PFX, "/api/v1").

start() ->
    {ok,User} = freya:get_env(http_basic_auth_user),
    {ok,Pass} = freya:get_env(http_basic_auth_pass),
    HTTPBasicAuth = {http_basic_auth_required,
                     [{user, User},{pass, Pass}]},
    Routes = [
              {?PFX++"/metrics/ns", freya_rest_ns, []},
              {?PFX++"/metrics/ns/:ns", freya_rest_names, []},
              {?PFX++"/metrics/ns/:ns/:metric_name", freya_rest_dps, []},

              {?PFX++"/health", freya_rest_health, []},
              {?PFX++"/admin/node/:cmd", freya_rest_admin_node, [HTTPBasicAuth]}
             ],

    Dispatch = cowboy_router:compile([ {'_', Routes} ]),
    Middlewares = [cowboy_router,
                   freya_cors_middleware,
                   cows_basic_auth_middleware,
                   cowboy_handler],
    Port = freya:get_env(http_port, ?DEFAULT_PORT),
    {ok, _} = cowboy:start_http(http, 100,
                                [{port, Port}],
                                [ {env, [{dispatch, Dispatch}]},
                                  {middlewares, Middlewares}
                                ]),
    ok.
