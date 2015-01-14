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
    {ok, _} = ranch:start_listener(freya_tcp_w, 1000, ranch_tcp,
                                   [{port,
                                     Port},{active,false},{sndbuf,1024*100},{rcvbuf,1024*100}], ?MODULE, Opts),
    ok.


start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, Opts) ->
    ok = ranch:accept_ack(Ref),
    Pub = kvlists:get_value(publisher, Opts),
    Vsn = kvlists:get_value(version, Opts),
    freya_tcp_status:inc(connections),
    loop(#proto{socket=Socket, transport=Transport,
                publisher=Pub, version=iolist_to_binary(Vsn)}).

loop(State = #proto{socket=Sock, transport=Trans, codec=Codec}) ->
    ok = Trans:setopts(Sock, [{active,once},{packet,2}]),
    receive
        {tcp, Sock, Data} ->
            lager:info("Received on socket ~p", [Data]),
            T = quintana:begin_timed(<<"freya.tcp.handle_packets">>),
            {ok, Packets} = Codec:decode(Data),
            {ok, NewState} = handle_packets(Packets, State),
            quintana:notify_timed(T),
            loop(NewState);
        Other ->
            lager:notice("Clossing connection: ~p", [Other]),
            freya_tcp_status:dec(connections),
            ok = Trans:close(Sock)
    end.

handle_packets([], State) ->
    {ok, State};
handle_packets([Packet|Remaining],
               State = #proto{codec=Codec, transport=Trans, socket=Sock}) ->
    case freya_tcp_proto:inbound(Packet, State) of
        {noreply, MaybeNewState} ->
            handle_packets(Remaining, MaybeNewState);
        {reply, Reply, MaybeNewState} ->
            Ack = Codec:encode(Reply),
            ok = gen_tcp:send(Sock, Ack),
            handle_packets(Remaining, MaybeNewState);
        {error, Reason} ->
            lager:error("Freya protocol error: ~p", [Reason]),
            freya_tcp_status:dec(connections),
            ok = Trans:close(State#proto.socket)
    end.
