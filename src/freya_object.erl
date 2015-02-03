-module(freya_object).

-include("freya_object.hrl").

-export([new/6, update/3]).
-export([merge/1, needs_repair/2, equal/2]).
-export([value/1]).

%%%===================================================================
%%% API
%%%===================================================================

new(Coordinator, Metric, Tags, Ts, {Fun, Precision}, Val) ->
    VC0 = vclock:fresh(),
    VC = vclock:increment(Coordinator, VC0),
    #freya_object{vclock=VC, metric=Metric, tags=Tags, ts=Ts,
                  fn=Fun, precision=Precision,
                  values=dict:from_list([{Coordinator, Val}])}.

update(Coordinator, Val0, #freya_object{fn=Fun, vclock=VC0, values=Values0}=Obj) ->
    Val = case dict:find(Coordinator, Values0) of
              {ok, #val{}=Val1} ->
                  merge_values(Fun, Val0, Val1);
              error ->
                  Val0
          end,
    VC = vclock:increment(Coordinator, VC0),
    Values = dict:store(Coordinator, Val, Values0),
    Obj#freya_object{values=Values, vclock=VC}.

merge([not_found|_]=Objs) ->
    P = fun(X) -> X =:= not_found end,
    case lists:all(P, Objs) of
        true -> not_found;
        false -> merge(lists:dropwhile(P, Objs))
    end;
merge([#freya_object{}=Obj|_]=Objs) ->
    case children(Objs) of
        [] ->
            not_found;
        [Child] ->
            Child;
        Children ->
            Values   = reconcile(   lists:map(fun values/1, Children)),
            MergedVC = vclock:merge(lists:map(fun vclock/1, Children)),
            Obj#freya_object{values=Values, vclock=MergedVC}
    end.

value(#freya_object{fn=Fn, values=Pairs0}) ->
    Pairs = dict:to_list(Pairs0),
    Values = lists:map(fun({_, Val}) -> Val end, Pairs),
    #val{value=Val} = reduce(fun(Val1, Val2) ->
                                merge_values(Fn, Val1, Val2)
                             end, Values),
    Val;
value(not_found) ->
    not_found.


needs_repair(Obj, Objs) ->
    lists:any(different(Obj), Objs).

equal(#freya_object{vclock=VC1}, #freya_object{vclock=VC2}) ->
    vclock:equal(VC1, VC2);
equal(not_found, not_found) -> true;
equal(_, _) -> false.

%%%===================================================================
%%% Internal functions
%%%===================================================================
merge_values(sum, #val{value=S0, points=C0}, #val{value=S1, points=C1}) ->
    #val{value=S0+S1, points=C0+C1};
merge_values(avg, #val{value=S0, points=C0}, #val{value=S1, points=C1}) ->
    {Accumulate, _} = freya_utils:aggregator_funs(avg),
    S = lists:foldl(fun(_, Acc) -> Accumulate(S0, Acc) end, {S1, C1},
                    lists:seq(1, C0)),
    #val{value=S, points=C0+C1};
merge_values(max, #val{value=S0, points=C0}, #val{value=S1, points=C1}) when S0 >= S1 ->
    #val{value=S0, points=C0+C1};
merge_values(max, #val{value=S0, points=C0}, #val{value=S1, points=C1}) when S0 < S1 ->
    #val{value=S1, points=C0+C1};
merge_values(min, #val{value=S0, points=C0}, #val{value=S1, points=C1}) when S0 =< S1 ->
    #val{value=S0, points=C0+C1};
merge_values(min, #val{value=S0, points=C0}, #val{value=S1, points=C1}) when S0 > S1 ->
    #val{value=S1, points=C0+C1}.

vclock(#freya_object{vclock=VC}) ->
    VC.

values(#freya_object{values=Values}) ->
    Values.

children(Objs) ->
    unique(Objs) -- ancestors(Objs).

unique(Objs) ->
    F = fun(not_found, Acc) -> Acc;
           (Obj, Acc) ->
                case lists:any(equal(Obj), Acc) of
                    true -> Acc;
                    false -> [Obj|Acc]
                end
        end,
    lists:foldl(F, [], Objs).

equal(Obj1) ->
    fun(Obj2) -> equal(Obj1, Obj2) end.

different(Obj1) ->
    fun(Obj2) -> not equal(Obj1, Obj2) end.

ancestors(Objs0) ->
    Objs = lists:filter(fun(Obj) -> Obj =/= not_found end, Objs0),
    As = [ [Obj2 || Obj2 <- Objs, ancestor(Obj2#freya_object.vclock,
                                           Obj1#freya_object.vclock)]
           || Obj1 <- Objs],
    unique(lists:flatten(As)).

ancestor(VC1, VC2) ->
    vclock:descends(VC2, VC1) andalso (vclock:descends(VC1, VC2) == false).

reconcile(Values0) ->
    Values = lists:map(fun dict:to_list/1, Values0),
    ValuesDict0 = lists:foldl(
                    fun({Coordinator, Value}, D) ->
                            dict:append(Coordinator, Value, D) end,
                    dict:new(), lists:flatten(Values)),
    dict:map(
      fun(_Coordinator, Vals0) ->
        [Val|_] = lists:sort(
                    fun(#val{points=C1}, #val{points=C2}) when C1 > C2 ->
                            true;
                        (_, _) -> false
                    end, Vals0),
        Val
      end, ValuesDict0).

reduce(_Fn, []) -> {error, empty};
reduce(_Fn, [V]) -> V;
reduce(Fn, [V1,V2|Rest]) -> reduce(Fn, [Fn(V1,V2)|Rest]).
