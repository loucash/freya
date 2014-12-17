-module(freya_cass_writer).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          write_delay,
          subscriber,
          timer
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Publisher) ->
    {ok, BatchSize} = freya:get_env(write_batch_size),
    {ok, WriteDelay} = freya:get_env(write_delay),
    {ok, Subscriber} = eqm:start_subscriber(Publisher, BatchSize, notify_full),
    Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
    {ok, #state{subscriber=Subscriber, write_delay=WriteDelay, timer=Timer}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({mail, _, _Msgs, _}, #state{write_delay=WriteDelay}=State) ->
    % insert batch Msgs
    Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
    {noreply, State#state{timer=Timer}};
handle_info({mail, _, buffer_full}, #state{timer=TimerRef,
                                           subscriber=Subscriber}=State) ->
    erlang:cancel_timer(TimerRef),
    eqm_sub:active(Subscriber),
    {noreply, State};
handle_info(flush_buffer_timeout, #state{write_delay=WriteDelay,
                                         subscriber=Subscriber}=State) ->
    case eqm_sub:info(Subscriber) of
        {ok, [_, {size, 0}]} ->
            Timer = erlang:send_after(WriteDelay, self(), flush_buffer_timeout),
            {noreply, State#state{timer=Timer}};
        {ok, _} ->
            eqm_sub:active(Subscriber),
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
