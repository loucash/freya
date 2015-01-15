-module(freya_tcp_status).

-behaviour(gen_server).

-export([start_link/0]).

-export([inc/1,dec/1,inc/2,dec/2]).
-export([query/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

inc(Counter) ->
    inc(Counter, 1).

inc(Counter, N) ->
    ets:update_counter(freya_stats, Counter, N).

dec(Counter) ->
    dec(Counter, -1).

dec(Counter, N) ->
    ets:update_counter(freya_stats, Counter, N).

query(Counter) ->
    [{Counter, V}] = ets:lookup(freya_stats, Counter),
    {ok, V}.

init([]) ->
    freya_stats = ets:new(freya_stats, [named_table, public, {read_concurrency,true}]),
    ets:insert(freya_stats, [{connections, 0},
                             {metrics_saved,0},
                             {metrics_dropped, 0}]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
