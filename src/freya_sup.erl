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
    StatsVNode   = {freya_stats_vnode_master,
                    {riak_core_vnode_master, start_link, [freya_stats_vnode]},
                    permanent, 5000, worker, [riak_core_vnode_master]},
    PushFSM = {freya_push_fsm_sup,
               {freya_push_fsm_sup, start_link, []},
               permanent, infinity, supervisor, [freya_push_fsm_sup]},
    GetFSM = {freya_get_fsm_sup,
               {freya_get_fsm_sup, start_link, []},
               permanent, infinity, supervisor, [freya_get_fsm_sup]},
    {ok, {{one_for_one, 5, 10},
          [?CHILD(freya_rollup_cfg, freya_rollup_cfg, worker, []),
           StatsVNode, PushFSM, GetFSM,
           ?CHILD(freya_rollup_topsup, freya_rollup_topsup, supervisor, []),
           ?CHILD(freya_snapshot_topsup, freya_snapshot_topsup, supervisor, [])
          ]
         }}.
