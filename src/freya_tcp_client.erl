-module(freya_tcp_client).

-behaviour(gen_fsm).

%% API
-export([start_link/0, start_link/1, start_link/3]).
-export([stop/1]).
-export([put_metric/5]).
-export([version/1]).

%% mainly for mocking
-export([send/2]).

%% gen_fsm callbacks
-export([init/1,
         connecting/2,
         connecting/3,
         connected/2,
         connected/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, { host          :: string(),
                 port          :: non_neg_integer(),
                 socket        :: port(),
                 opts          :: term() }).

-define(TCP_OPTS, [binary,{active, false},
                   {packet, line},{keepalive, true}]).

-define(RECONNECT_TIME_MSECS , 1000).
-define(TCP_RECV_LEN         , 0).
-define(TCP_RECV_TIMEOUT     , 2500).

start_link() ->
    {H, P} = endpoint(),
    start_link(H, P, []).

start_link(Opts) when is_list(Opts) ->
    {H, P} = endpoint(),
    start_link(H, P, Opts).

start_link(Host, Port, Opts) ->
    gen_fsm:start_link(?MODULE, [Host, Port, Opts], []).

stop(Conn) ->
    gen_fsm:sync_send_all_state_event(Conn, shutdown).

put_metric(Conn, M, TS, V, Tags) when is_pid(Conn) ->
    gen_fsm:sync_send_event(Conn, {put_metric, {M, TS, V, Tags}}).

version(Conn) ->
    gen_fsm:sync_send_event(Conn, version).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([H, P, _]) ->
    process_flag(trap_exit, true),
    S = #state{host = H, port = P, opts=?TCP_OPTS},
    try_connect(S).

try_connect(S = #state{host=H, port=P, opts=Opts}) ->
    case connect(S, Opts) of
        {ok, S2} ->
            {ok, connected, S2};
        {error, R} ->
            lager:warning("Could not connect to freya at ~s:~w due to ~p",
                         [H, P, R]),
            _ = schedule_reconnect(),
            {ok, connecting, S}
    end.

connecting(reconnect, S1=#state{}) ->
    case try_connect(S1) of
        {ok, connected, S2} ->
            {next_state, connected, S2};
        {ok, connecting, S1} ->
            _ = schedule_reconnect(),
            {next_state, connecting, S1}
    end;
connecting(ping, S1) ->
    {next_state, connecting, S1}.

connecting(_Event, _From, State) ->
    Reply = {error, connecting},
    {reply, Reply, connecting, State}.

connected(Msg, S1) ->
    error({unknown_message, Msg}).

connected(version, _From, S1) ->
    Reply = freya_version(S1),
    {reply, Reply, connected, S1};
connected({put_metric = Cmd, {M, TS, V, Tags}}, _From, S1) ->
    Raw = freya_tcp_codec:encode({command, M, TS, V, Tags}),
    case send(Raw, S1) of
        ok ->
            {reply, ok, connected, S1};
        {error, _}=E ->
            _ = schedule_reconnect(),
            S2 = close_connection(S1),
            {reply, E, connecting, S2}
    end;
connected(_Event, _From, State) ->
    Reply = {error, unknown_call},
    {reply, Reply, connected, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(shutdown, _From, _, #state{socket=undefined}=S1) ->
    {stop, normal, ok, S1};
handle_sync_event(shutdown, _From, _, #state{}=S1) ->
    close_connection(S1),
    {stop, normal, ok, S1};
handle_sync_event(Event, _From, StateName, State) ->
    Reply = {error, {unknown_event, Event}},
    {reply, Reply, StateName, State}.

handle_info({'EXIT', _, _}=Info, _StateName, S1) ->
    close_connection(S1),
    {stop, {crash, Info}, S1};
handle_info(Info, _, S1) ->
    lager:error("Unexpected message: ~p", [Info]),
    {noreply, S1}.

terminate(Reason, _StateName, #state{}=S1) ->
    lager:info("freya connection terminating: ~p", [Reason]),
    close_connection(S1),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

schedule_reconnect() ->
    lager:info("Reconnecting to freya in ~w", [?RECONNECT_TIME_MSECS]),
    gen_fsm:send_event_after(?RECONNECT_TIME_MSECS, reconnect).

close_connection(#state{socket=undefined}=State) ->
    State;
close_connection(#state{socket=Sock}=State) ->
    ok = gen_tcp:close(Sock),
    kai_pool:leave(),
    State#state{socket=undefined}.

freya_version(S1) ->
    Raw = freya_tcp_codec:version(),
    case send(Raw, S1) of
        ok ->
            case freya_wait_reply(S1) of
                Data when is_binary(Data) ->
                    {ok, {freya, Data}};
                {error, _}=E ->
                    E
            end;
        {error, R} ->
            {error, {freya, R}}
    end.

freya_wait_reply(#state{socket = Socket}) ->
    case gen_tcp:recv(Socket, ?TCP_RECV_LEN, ?TCP_RECV_TIMEOUT) of
        {ok, Data} ->
            Data;
        {error, _}=E ->
            E
    end.

connect(#state{host=H, port=P} = S, Opts) ->
    lager:info("Connecting to freya at ~s:~p", [H, P]),
    case gen_tcp:connect(H, P, Opts) of
        {ok, Socket} ->
            lager:info("Connected to freya at ~s:~p", [H, P]),
            {ok, S#state{socket = Socket}};
        {error, _} = E ->
            E
    end.

send(_Raw, #state{socket=undefined}) ->
    {error, not_connected};
send(Raw, #state{socket=Socket}) ->
    gen_tcp:send(Socket, Raw).

endpoint() ->
    H = freya:get_env(tcp_api_host, "localhost"),
    P = freya:get_env(tcp_api_port, 7055),
    {H,P}.