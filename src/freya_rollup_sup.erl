-module(freya_rollup_sup).
-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/1,
         terminate_child/1]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(Args) ->
    supervisor:start_child(?MODULE, Args).

terminate_child(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Spec = {freya_rollup_wrk,
            {freya_rollup_wrk, start_link, []},
            temporary, infinity, worker, [freya_rollup_wrk]},
    {ok, {{simple_one_for_one, 10, 10}, [Spec]}}.
