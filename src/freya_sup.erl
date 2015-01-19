-module(freya_sup).

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
    VMaster= {freya_vnode_master,
              {riak_core_vnode_master, start_link, [freya_vnode]},
              permanent, 5000, worker, [riak_core_vnode_master]},
    Rollups= {freya_rollups_vnode_master,
              {riak_core_vnode_master, start_link, [freya_rollups_vnode]},
              permanent, 5000, worker, [riak_core_vnode_master]},
    {ok, {{one_for_one, 5, 10},
          [VMaster, Rollups,
           ?CHILD(freya_tcp_status, freya_tcp_status, worker, [])]
         }}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
