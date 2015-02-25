-module(freya_rollup_cfg).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([match/2, match/3, compile/1]).

%% For tests
-export([do_match/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

match(Name, Tags) ->
    match(default, Name, Tags).

match(Profile, Name, Tags) ->
    case ets:lookup(?MODULE, cfg) of
        [] -> [];
        [{cfg, Dispatch}] ->
            do_match(Dispatch, Profile, Name, Tags)
    end.

do_match(Dispatch, Profile, Name, Tags) ->
    case proplists:get_value(Profile, Dispatch) of
        undefined -> [];
        Rules ->
            lists:usort(lists:filtermap(fun(F) -> F(Name, Tags) end, Rules))
    end.


compile(Config) ->
    compile_profiles(Config, []).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    _ = ets:new(?MODULE, [named_table, protected, {read_concurrency, true}]),
    self() ! reload,
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(reload, State) ->
    RollupCfg = freya:get_env(rollup_config, []),
    Dispatch = compile(RollupCfg),
    true = ets:insert(?MODULE, {cfg, Dispatch}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
compile_profiles([], Acc) ->
    Acc;
compile_profiles([{Profile, Rules}|Rest], Acc) ->
    RulesFns = compile_rules(Rules),
    compile_profiles(Rest, [{Profile, RulesFns}|Acc]).

compile_rules(Rules) ->
    lists:map(fun compile_rule/1, Rules).

compile_rule({Metric1, Tags1, Aggregate, Options}) ->
    MetricFn = compile_metric(Metric1),
    TagFns   = compile_tags(Tags1),
    fun(Metric2, Tags2) ->
        case matches_metric(Metric2, MetricFn) andalso
             matches_tags(Tags2, TagFns) of
            true ->
                {true, {Aggregate, Options}};
            false ->
                false
        end
    end.

compile_metric({_Ns, Metric}) ->
    compile_metric(Metric);
compile_metric('_') ->
    fun(_) -> true end;
compile_metric(M1) ->
    fun(M2) when M1 =:= M2 -> true;
       ({_, M2}) when M1 =:= M2 -> true;
       (_) -> false
    end.

compile_tags('_') ->
    [];
compile_tags(Tags) ->
    lists:map(fun compile_tag/1, Tags).

compile_tag({'_', '_'}) ->
    fun({_,_}) -> true end;
compile_tag({'_', V1}) ->
    fun({_, V2}) when V1 =:= V2 -> true;
       ({_, _}) -> false
    end;
compile_tag({N1, '_'}) ->
    fun({N2, _}) when N1 =:= N2 -> true;
       ({_, _}) -> false
    end;
compile_tag({N1, V1}) ->
    fun({N2, V2}) when N1 =:= N2 andalso V1 =:= V2 -> true;
       ({_, _}) -> false
    end.

matches_metric(Metric, Fn) ->
    Fn(Metric).

matches_tags(_Tags, []) -> true;
matches_tags(Tags, Fns) ->
    lists:all(fun(F) -> lists:any(F, Tags) end, Fns).
