-module(freya_tcp).
-behaviour(ranch_protocol).

% api
-export([start/0]).

% ranch callbacks
-export([start_link/4]).
-export([init/4]).


-define(DEFAULT_PORT, 7055).
-define(DEFAULT_CODEC, freya_tcp_codec).

-record(state, {codec = ?DEFAULT_CODEC :: module()}).

start() ->
    Port = freya:get_env(w_tcp_port, ?DEFAULT_PORT),
    {ok, _} = ranch:start_listener(w_tcp, 100, ranch_tcp,
                                   [{port, Port}], ?MODULE, []),
    ok.

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts=[]) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, #state{}).

loop(Socket, Transport, State) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            case handle_raw_data(Data, State) of
                {ok, MaybeNewState} ->
                    Ack = (State#state.codec):encode(ack),
                    Transport:send(Socket, Ack),
                    loop(Socket, Transport, MaybeNewState);
                {error, _Reason} ->
                    % TODO add lager
                    ok = Transport:close(Socket)
            end;
        _ ->
            ok = Transport:close(Socket)
    end.

handle_raw_data(RawData, State) ->
    Data = (State#state.codec):decode(RawData),
    handle_data(Data, State).

handle_data(Data, State) ->
    io:format("Received ~p~n", [Data]),
    {ok, State}.
