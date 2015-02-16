-module(freya_rest_names).

-export([init/3]).
-export([rest_init/2]).
-export([content_types_provided/2]).
-export([allowed_methods/2]).
-export([resource_exists/2]).
-export([render_json/2]).
-export([render_msgpack/2]).
-export([content_types_accepted/2]).
-export([allow_missing_post/2]).
-export([put_dps_json/2]).
-export([put_dps_msgpack/2]).

-include("freya.hrl").

-define(dp, freya_data_point).
-define(w, freya_writer).

-record(state, {ns :: metric_ns(),
                payload :: binary(),
                names :: list(metric_name())}).

init(_Transport, _Req, []) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(R0, _Opts) ->
    {Ns, R1} = cowboy_req:binding(ns, R0),
    {ok, Body, R2} = cowboy_req:body(R1),
    {ok, R2, #state{ns=Ns, payload=Body}}.

content_types_provided(Req, State) ->
    ContentTypes = [{<<"application/x-msgpack">>, render_msgpack},
                    {<<"application/json">>, render_json}],
    {ContentTypes, Req, State}.

content_types_accepted(Req, State) ->
    ContentTypes = [{<<"application/x-msgpack">>, put_dps_msgpack},
                    {<<"application/json">>, put_dps_json}],
    {ContentTypes, Req, State}.

allowed_methods(R0, State) ->
    {[<<"GET">>, <<"POST">>], R0, State}.

resource_exists(Req, State=#state{ns = Ns, payload = <<>>}) ->
    case freya_reader:metric_names(Ns) of
        {ok, Names} ->
            {true, Req, State#state{names=Names}};
        {error, not_found} ->
            {false, Req, State}
    end;
resource_exists(Req, State) ->
    {false, Req, State}.

allow_missing_post(Req, State) ->
    {true, Req, State}.

render_msgpack(Req, State=#state{names=Names}) ->
    Resp = msgpack:pack(Names, [{format, jsx}]),
    {Resp, Req, State}.

render_json(Req, State=#state{names=Names}) ->
    Resp = jsx:encode(Names),
    {Resp, Req, State}.

put_dps_msgpack(R0, S0=#state{ns=Ns, payload=Body}) ->
    {ok, Payload} = msgpack:unpack(Body, [{format,jsx}]),
    {put_metrics(Ns, Payload), R0, S0}.

put_dps_json(R0, S0=#state{ns=Ns, payload=Body}) ->
    Payload = jsx:decode(Body),
    {put_metrics(Ns, Payload), R0, S0}.

put_metrics(Ns, Payload) ->
    Res = lists:filtermap(fun(M) ->
                                  Name = kvlists:get_value(<<"name">>, M),
                                  Points = kvlists:get_value(<<"points">>, M),
                                  T0 = kvlists:get_value(<<"tags">>, M, []),
                                  Tags = [ {K,V} || [{K,V}] <- T0 ],
                                  case save_dps(Name, Ns, Points, Tags) of
                                      [] ->
                                          false;
                                      Failed -> 
                                          {true, {Name, Failed, Tags}}
                                  end
                          end, Payload),
    Res == [].

save_dps(Name, Ns, TsVs, Tags) ->
    {ok, Pub} = ?w:publisher(),
    lists:filtermap(fun([Ts,V]) ->
                            DP = ?dp:new({Ns,Name}, Ts, V, Tags),
                            case ?w:save(Pub, DP) of
                                ok ->
                                    false;
                                {error, no_capacity} ->
                                    {true, [Ts,V]}
                            end
                    end, TsVs).
