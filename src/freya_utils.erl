-module(freya_utils).

-include("freya.hrl").
-define(DEFAULT_PMAP_TIMEOUT, 5000).

-export([floor/2, ceil/2, prev/2, next/2, ms/1]).
-export([aggregator_funs/1, aggregate_key/4]).
-export([sanitize_tags/1, sanitize_name/1]).
-export([row_width/1]).
-export([wait_for_reqid/2]).
-export([pmap/3, pmap/4]).
-export([ring_member_status/0,
         ring_member_status/1,
         ring_pending_claim_percentage/2]).

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
    lists:foldl(FoldTagsFun, orddict:new(), lists:usort(Tags)).

sanitize_name(Name) when is_binary(Name) ->
    {?DEFAULT_NS, Name}.

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

aggregate_key({Ns, Name}, Tags, Ts, {Fun, Precision}) ->
    Part1 = [{<<"ns">>, Ns},
             {<<"name">>, Name}],
    Part2 = [{<<"tags">>, Tags},
           {<<"ts">>, Ts},
           {<<"fun">>, atom_to_binary(Fun, utf8)},
           {<<"precision">>, freya_utils:ms(Precision)}],
    {msgpack:pack(Part1, [{format,jsx}]),
     msgpack:pack(Part2, [{format,jsx}])};
aggregate_key(Name, Tags, Ts, Aggregate) ->
    aggregate_key({?DEFAULT_NS, Name}, Tags, Ts, Aggregate).

wait_for_reqid(ReqId, Timeout) ->
    receive
        {ok, ReqId} -> ok;
        {ok, ReqId, Value} -> {ok, Value};
        {error, ReqId, Reason} -> {error, Reason}
    after Timeout ->
              {error, timeout}
    end.

ring_member_status() ->
    ring_member_status(node()).

ring_member_status(Node) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    case riak_core_ring:member_status(Ring, Node) of
        leaving ->
            {ok, {leaving, ring_pending_claim_percentage(Ring, Node)}};
        valid ->
            case ring_pending_claim_percentage(Ring, Node) of
                100.0 ->
                    {ok, valid};
                PercentDone ->
                    {ok, {valid, PercentDone}}
            end;
        invalid ->
            {error, invalid};
        Status ->
            {ok, Status}
    end.

ring_pending_claim_percentage(Ring, Node) ->
    {RingPercent, NextPercent} = riak_core_console:pending_claim_percentage(Ring, Node),
    PercentDone = 100.0 - abs(NextPercent - RingPercent),
    PercentDone.
