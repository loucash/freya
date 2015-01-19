-module(freya_rest_dps).

-include("freya.hrl").

-export([init/3]).
-export([rest_init/2]).
-export([content_types_provided/2]).
-export([content_types_accepted/2]).
-export([allowed_methods/2]).
-export([metrics_query_of_json/2]).

-record(query, {
          raw_query :: jsx:json_term(),
          results   :: jsx:json_term()
         }).

init(_Transport, _Req, _Opts) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(Req, []) ->
    case cowboy_req:body(Req) of
        {ok, Body, Req2} ->
            {ok, Req2, #query{raw_query=Body}}
    end.

allowed_methods(Req, State) ->
    {[<<"POST">>, <<"OPTIONS">>], Req, State}.

content_types_provided(Req, #query{}=S) ->
    {[ {<<"application/json">>, metrics_query_json} ], Req, S}.

content_types_accepted(Req, S) ->
    {[ {<<"application/json">>, metrics_query_of_json} ], Req, S}.

metrics_query_of_json(Req, #query{raw_query=Raw}=S) ->
    % tyranny begins
    Q = jsx:decode(Raw),
    T0 = determine_time({<<"start_absolute">>, <<"start_relative">>}, Q),
    T1 = determine_time({<<"end_absolute">>, <<"end_relative">>}, Q),
    Metrics = kvlists:get_value(<<"metrics">>, Q),
    Searches = query_to_searches({T0,T1}, Metrics),
    DPs = perform_searches(Searches),
    Response = render_response(DPs),
    Req2 = cowboy_req:set_resp_body(Response, Req),
    {true, Req2, S}.

render_response(DPset) ->
    Resp = [{queries, [ [{results, [ dps_to_results(DPs) || DPs <- DPset ]} ] ] }],
    jsx:encode(Resp).

dps_to_results({Name, DPs}) ->
    Values = [ [TS,V] || #data_point{ts=TS,value=V} <- DPs ],
    [{name, Name}, {values, Values},
     {group_by, [ [{name,type},{type,number}] ]}].

query_to_searches({T0,T1}, Metrics) ->
    [ [{start_time, T0},
       {end_time, T1},
       {metric_name, kvlists:get_value(<<"name">>, M)} ]
      || M <- Metrics ].

perform_searches(Searches) ->
    lists:filtermap(fun(Search) ->
                            Name = kvlists:get_value(metric_name, Search),
                            case freya_reader:search(Search) of
                                {error, not_found} -> false;
                                {ok, DPs}          -> {true, {Name, DPs}}
                            end
                    end, Searches).

determine_time({AbsPath,RelPath}, J) ->
    case kvlists:get_value(AbsPath, J) of
        undefined ->
            case kvlists:get_value(RelPath, J) of
                undefined=U ->
                    U;
                Props ->
                    Fields = [<<"value">>, <<"unit">>],
                    [V0,U0] = kvlists:get_values(Fields, Props),
                    V1 = binary_to_integer(V0),
                    vu_to_ms(V1, U0)
            end;
        TimeMs when is_integer(TimeMs) ->
            TimeMs
    end.

vu_to_ms(V, <<"milliseconds">>) -> V;
vu_to_ms(V, <<"seconds">>)      -> timer:seconds(V);
vu_to_ms(V, <<"minutes">>)      -> timer:minutes(V);
vu_to_ms(V, <<"hours">>)        -> timer:hours(V);
vu_to_ms(V, <<"days">>)         -> timer:hours(24) * V;
vu_to_ms(V, <<"months">>)       -> timer:hours(24) * 30 * V; %% FIXME
vu_to_ms(V, <<"years">>)        -> timer:hours(24) * 365 * V.
