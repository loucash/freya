-module(freya_snapshot_topsup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([]) ->
    Router = {freya_snapshot,
              {freya_snapshot, start_link, []},
              permanent, infinity, worker, [freya_snapshot]},
    Workers = {freya_snapshot_sup,
               {freya_snapshot_sup, start_link, []},
               permanent, infinity, supervisor, [freya_snapshot_sup]},
    {ok, {{rest_for_one, 10, 10},
          [Router, Workers]}}.
