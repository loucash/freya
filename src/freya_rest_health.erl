-module(freya_rest_health).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).


init(_Type, Req, []) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {ok, Vsn} = application:get_key(freya, vsn),
    CompileTime = kvlists:get_path([compile, time], freya:module_info()),
    {Y,M,D,Hr,Min,Sec} = CompileTime,
    BuildDate = tic:datetime_to_iso8601({{Y,M,D},{Hr,Min,Sec}}),
    Body = jsx:encode([{<<"version">>, list_to_binary(Vsn)},
                       {<<"build_date">>, BuildDate}]),
    Headers = [{<<"content-type">>, <<"application/json">>}],
    {ok, Req2} = cowboy_req:reply(200, Headers, Body, Req),
    {ok, Req2, State}.

terminate(_Reason, _Req, _State) ->
    ok.
