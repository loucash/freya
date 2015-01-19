-module(freya_rest_metrics).

-export([init/3]).
-export([rest_init/2]).
-export([content_types_provided/2]).
-export([allowed_methods/2]).
-export([metrics_list_json/2]).

-record(metrics, {
          names = [] :: [binary()]
         }).

init(_Transport, _Req, _Opts) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, []) ->
    case freya_reader:metric_names() of
        {ok, Names} ->
            {ok, Req, #metrics{names=Names}};
        {error, Reason} ->
            lager:error("Error while retrieving metrics list: ~p", [Reason]),
            {halt, Req, 500}
    end.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"OPTIONS">>], Req, State}.

content_types_provided(Req, #metrics{}=S) ->
    {[ {<<"application/json">>, metrics_list_json} ], Req, S}.

metrics_list_json(Req, #metrics{names=Names}=S) ->
    Body = jsx:encode([{<<"results">>, Names}]),
    {Body, Req, S}.
