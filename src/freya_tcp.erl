-module(freya_tcp).
-behaviour(ranch_protocol).

% api
-export([start/1]).

% ranch callbacks
-export([start_link/4]).
-export([init/4]).

-include("freya_tcp.hrl").

-spec start(eqm:pub()) -> ok.
start(Publisher) ->
    {ok, Version} = freya:version(),
    Port = freya:get_env(w_tcp_port, ?DEFAULT_PORT),
    Opts = [{publisher, Publisher}, {version, Version}],

    lager:info("Starting TCP freya interface ~s / ~p", [Version, Publisher]),
    {ok, _} = ranch:start_listener(freya_tcp_w, 100, ranch_tcp,
                                   [{port, Port}], ?MODULE, Opts),
    ok.


start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    Pub = kvlists:get_value(publisher, Opts),
    Vsn = kvlists:get_value(version, Opts),
    loop(Socket, Transport, #proto{publisher=Pub, version=Vsn}).

loop(Socket, Transport, State) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            case handle_raw_data(Data, State) of
                {noreply, MaybeNewState} ->
                    loop(Socket, Transport, MaybeNewState);
                {reply, Reply, MaybeNewState} ->
                    Ack = (State#proto.codec):encode(Reply),
                    ok = Transport:send(Socket, Ack),
                    loop(Socket, Transport, MaybeNewState);
                {error, Reason} ->
                    lager:error("Freya protocol error: ~p", [Reason]),
                    ok = Transport:close(Socket)
            end;
        _ ->
            ok = Transport:close(Socket)
    end.

handle_raw_data(RawData, State) ->
    Data = (State#proto.codec):decode(RawData),
    freya_tcp_proto:inbound(Data, State).
