-module(freya_rest_ns).

-export([init/3]).
-export([rest_init/2]).
-export([content_types_provided/2]).
-export([allowed_methods/2]).
-export([resource_exists/2]).
-export([render_json/2]).
-export([render_msgpack/2]).

-include("freya.hrl").

-record(state, {namespaces :: list(metric_ns())}).

init(_Transport, _Req, []) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(Req1, _Opts) ->
    {ok, Req1, #state{}}.

content_types_provided(Req, State) ->
    ContentTypes = [{<<"application/x-msgpack">>, render_msgpack},
                    {<<"application/json">>, render_json}],
    {ContentTypes, Req, State}.

allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

resource_exists(Req, State) ->
    {ok, Nss} = freya_reader:namespaces(),
    {true, Req, State#state{namespaces=Nss}}.

render_msgpack(Req, State=#state{namespaces=Nss}) ->
    Resp = msgpack:pack([{<<"namespaces">>, Nss}], [{format, jsx}]),
    {Resp, Req, State}.

render_json(Req, State=#state{namespaces=Nss}) ->
    Resp = jsx:encode([{<<"namespaces">>, Nss}]),
    {Resp, Req, State}.
