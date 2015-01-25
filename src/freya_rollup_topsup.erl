-module(freya_rollup_topsup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Id, Mod, Type, Args), {Id, {Mod, start_link, Args},
                                     permanent, 5000, Type, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Router = {freya_rollup,
              {freya_rollup, start_link, []},
              permanent, infinity, worker, [freya_rollup]},
    Workers = {freya_rollup_sup,
               {freya_rollup_sup, start_link, []},
               permanent, infinity, supervisor, [freya_rollup_sup]},
    {ok, {{rest_for_one, 10, 10},
          [Router, Workers]}}.
