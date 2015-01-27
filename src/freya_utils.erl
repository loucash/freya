-module(freya_utils).

-include("freya.hrl").
-define(DEFAULT_PMAP_TIMEOUT, 5000).

-export([floor/2, ceil/2, prev/2, next/2, ms/1]).
-export([aggregator_funs/1, aggregate_key/4]).
-export([sanitize_tags/1]).
-export([row_width/1]).
-export([wait_for_reqid/2]).
-export([pmap/3, pmap/4]).

-spec floor(milliseconds(), precision() | milliseconds()) ->
    milliseconds().
floor(MSec, Sample) when is_tuple(Sample) ->
    floor(MSec, ms(Sample));
floor(MSec, Sample) ->
    MSec - (MSec rem Sample).

-spec ceil(milliseconds(), precision() | milliseconds()) ->
    milliseconds().
ceil(MSec, Sample) ->
    next(floor(MSec, Sample), Sample).

-spec prev(milliseconds(), precision() | milliseconds()) ->
    milliseconds().
prev(Ts, Sample) when is_tuple(Sample) ->
    prev(Ts, ms(Sample));
prev(Ts, Sample) ->
    Ts - Sample.

-spec next(milliseconds(), precision() | milliseconds()) ->
    milliseconds().
next(Ts, Sample) when is_tuple(Sample) ->
    next(Ts, ms(Sample));
next(Ts, Sample) ->
    Ts + Sample.

-spec ms(precision() | milliseconds()) -> milliseconds().
ms({N, weeks}) ->
    N * ms({7, days});
ms({N, days}) ->
    N * timer:hours(24);
ms({N, Tp}) ->
    timer:Tp(N);
ms(N) when is_integer(N) ->
    N.

sanitize_tags(Tags) ->
    FoldTagsFun =
    fun({Name, Value}, D0) when is_list(Value) ->
        case orddict:find(Name, D0) of
            error ->
                orddict:store(Name, lists:sort(Value), D0);
            {ok, List} ->
                orddict:store(Name, lists:sort(Value ++ List), D0)
        end;
       ({Name, Value}, D0) ->
        case orddict:find(Name, D0) of
            error ->
                orddict:append(Name, Value, D0);
            {ok, List} ->
                orddict:store(Name, lists:sort([Value|List]), D0)
        end
    end,
    lists:foldl(FoldTagsFun, orddict:new(), unique(Tags)).

unique(L) ->
    sets:to_list(sets:from_list(L)).

pmap(F, Args, L) ->
    pmap(F, Args, L, ?DEFAULT_PMAP_TIMEOUT).

pmap(F, Args, L, Timeout) ->
    S = self(),
    Ref = erlang:make_ref(),
    Pids = lists:map(
             fun(I) ->
                spawn_link(fun() -> do_f(S, Ref, F, [I|Args]) end)
             end, L),
    gather(Pids, Ref, Timeout).

do_f(Parent, Ref, F, Args) ->
    Parent ! {self(), Ref, apply(F, Args)}.

gather([Pid|T], Ref, Timeout) ->
    receive
        {Pid, Ref, Ret} -> [Ret|gather(T, Ref, Timeout)]
    after Timeout ->
        unlink(Pid),
        exit(Pid, timeout),
        [{error, timeout}|gather(T, Ref, Timeout)]
    end;
gather([], _, _) ->
    [].

% @doc We calculate row width in weeks
-spec row_width(data_precision()) -> precision().
row_width(raw) ->
    calc_row_width(1);
row_width({_, Precision}) ->
    calc_row_width(Precision).

-spec calc_row_width(precision()) -> precision().
calc_row_width(Ms) when is_tuple(Ms) ->
    calc_row_width(ms(Ms));
calc_row_width(Ms) ->
    Weeks = (Ms * ?MAX_ROW_WIDTH) div ms({1, weeks}),
    {lists:min([?MAX_WEEKS, Weeks]), weeks}.

%% @doc Return a function that calculates aggregates
aggregator_funs(max) ->
    {fun(X, undefined) -> X;
        (X, Acc) when X > Acc -> X;
        (_, Acc) -> Acc end,
     fun(X) -> X end};
aggregator_funs(min) ->
    {fun(X, undefined) -> X;
        (X, Acc) when X < Acc -> X;
        (_, Acc) -> Acc end,
     fun(X) -> X end};
aggregator_funs(sum) ->
    {fun(X, undefined) -> X;
        (X, Acc) -> X + Acc end,
     fun(X) -> X end};
aggregator_funs(count) ->
    {fun(undefined) -> 1;
        (X) -> X + 1 end,
     fun(X) -> X end};
aggregator_funs(avg) ->
    {fun(X, undefined) -> {X, 1};
        (X, {Avg0, N0}) ->
             N    = N0 + 1,
             Diff = X - Avg0,
             Avg  = (Diff / N) + Avg0,
             {Avg, N}
     end,
     fun({X, _}) -> X end}.

aggregate_key(Metric, Tags, Ts, {Fun, Precision}) ->
    Key = [{<<"tags">>, Tags},
           {<<"ts">>, Ts},
           {<<"fun">>, atom_to_binary(Fun, utf8)},
           {<<"precision">>, freya_utils:ms(Precision)}],
    {Metric, msgpack:pack(Key, [{format,jsx}])}.

wait_for_reqid(ReqId, Timeout) ->
    receive
        {ok, ReqId} -> ok;
        {ok, ReqId, Value} -> {ok, Value};
        {error, ReqId, Reason} -> {error, Reason}
    after Timeout ->
              {error, timeout}
    end.
