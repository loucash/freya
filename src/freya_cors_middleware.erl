-module(freya_cors_middleware).

-behaviour(cowboy_middleware).

-export([execute/2]).


execute(Req, Env) ->
    Opts = proplists:get_value(handler_opts, Env),
    Cors = proplists:get_value(websocket_cors, Opts, false),
    {Origin, Req1} = cowboy_req:header(<<"origin">>, Req),
    reply(Origin, Cors, Req1, Env).


reply(undefined, true, Req, _) ->
    {error, 412, Req};
reply(undefined, false, Req, Env) ->
    {ok, Req, Env};
reply(Origin, true, Req, Env) ->
    {ok, Allowed} = freya:get_env(cors_domains),
    lager:debug("Websocket CORS get allowed domains: ~p", [Allowed]),
    case is_in_allowed(Origin, Allowed) of
        true  ->
            lager:debug("Websocket CORS check origin. Allowed: ~p", [Origin]),
            Req1 = set_cors_headers(Req, Origin),
            {ok, Req1, Env};
        false ->
            lager:debug("Websocket CORS check origin. Not allowed: ~p", [Origin]),
            {error, 412, Req}
    end;
reply(Origin, false, Req, Env) ->
    {ok, Allowed} = freya:get_env(cors_domains),
    lager:debug("CORS get allowed domains: ~p", [Allowed]),
    Req1 = case is_in_allowed(Origin, Allowed) of
               true  ->
                   lager:debug("CORS check origin. Allowed: ~p", [Origin]),
                   set_cors_headers(Req, Origin);
               false ->
                   lager:debug("CORS check origin. Not allowed: ~p", [Origin]),
                   Req
           end,
    {ok, Req1, Env}.



is_in_allowed(Origin, Allowed) ->
    AllowedTuples = domain_port_split(Allowed),

    SOrigin = binary_to_list(Origin),
    SchemeDefaults = [{wss,443},{ws,80} | http_uri:scheme_defaults()],

    case http_uri:parse(SOrigin, [{scheme_defaults, SchemeDefaults}]) of
        {ok, {_Scheme, _, Host, Port, _, _}} ->
            BinHost = list_to_binary(Host),
            BinPort = integer_to_binary(Port),
            is_in_allowed_tuples({BinHost, BinPort}, AllowedTuples);
        {error, _Reason} ->
            false
    end.

domain_port_split(Allowed) ->
    lists:map(fun(A) ->
                        Split = bstr:split(A, <<":">>),
                        case length(Split) of
                            2 -> list_to_tuple(Split);
                            _ -> {A, <<"*">>}
                        end
                end, Allowed).


is_in_allowed_tuples(_Origin, []) -> false;
is_in_allowed_tuples({OD, OP} = Origin, [{AD, AP} | T]) ->
    case is_domain_allowed(OD, AD) andalso is_port_allowed(OP, AP) of
        true -> true;
        false -> is_in_allowed_tuples(Origin, T)
    end.


is_domain_allowed(Origin, Allowed) ->
    OriginSplit = lists:reverse(bstr:split(Origin, <<".">>)),
    AllowedSplit = lists:reverse(bstr:split(Allowed, <<".">>)),
    check_domain_match(OriginSplit, AllowedSplit).

check_domain_match(_, [<<"*">>|_]) -> true;
check_domain_match([], []) -> true;
check_domain_match([], _) -> false;
check_domain_match(_ , []) -> false;
check_domain_match([H1|T1], [H2|T2]) ->
    lager:debug("~p ~p", [H1, H2]),
    case H1=:= H2 of
        true -> check_domain_match(T1, T2);
        false -> false
    end.

is_port_allowed(_ , <<"*">>) -> true;
is_port_allowed(OPort, APort) -> OPort =:= APort.


set_cors_headers(Req, Origin) ->
    SetHeader = fun({Header, Value}, R) ->
        cowboy_req:set_resp_header(Header, Value, R)
    end,
    Headers = cors_headers(Origin),
    lists:foldl(SetHeader, Req, Headers).

cors_headers(Origin) ->
    {ok, AllowHeaders} = freya:get_env(allow_headers),
    {ok, AllowMethods} = freya:get_env(allow_methods),
    {ok, MaxAge} = freya:get_env(max_age),
    {ok, AllowCredentials} = freya:get_env(allow_credentials),
    {ok, ExposeHeaders} = freya:get_env(expose_headers),
    [{<<"access-control-allow-origin">>      , Origin},
     {<<"access-control-allow-headers">>     , AllowHeaders},
     {<<"access-control-allow-methods">>     , AllowMethods},
     {<<"access-control-max-age">>           , MaxAge},
     {<<"access-control-allow-credentials">> , AllowCredentials},
     {<<"access-control-expose-headers">>    , ExposeHeaders}].
