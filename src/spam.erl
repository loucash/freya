-module(spam).
-compile([export_all]).


v() ->
    {ok,C} = freya_tcp_client:start_link(),
    freya_tcp_client:version(C).

s() ->
    {ok,C} = freya_tcp_client:start_link(),
    freya_tcp_client:status(C).

p() ->
    {ok,C} = freya_tcp_client:start_link(),
    ok = freya_tcp_client:put_metric(C, <<"test">>, 255).


spam(WrkCount, N) ->
    application:ensure_all_started(folsomite),
    application:ensure_all_started(quintana),
    Name1 = integer_to_binary(WrkCount),
    Name2 = integer_to_binary(N),
    T = quintana:begin_timed(<<"freya.spam.", Name1/binary, ".",
                               Name2/binary>>),
    io:format("Starting ~w workers, inserting ~w metrics.~n", [WrkCount, N]),
    Workers = [begin
                   {ok,C} = freya_tcp_client:start_link(),
                   C end
               || _ <- lists:seq(1, WrkCount) ],

    {ok,Reader} = freya_tcp_client:start_link(),
    {D0,S0,_C0} = get_status(Reader),

    io:format("Workers spawned.~n"),
    Metrics = [{metric(), random:uniform()} || _ <- lists:seq(1, N)],
    io:format("Metrics generated.~n"),
    {Us, done} = timer:tc(fun() -> round_robin(Workers, Metrics) end),
    Time = Us * 0.000001,
    quintana:notify_timed(T),
    io:format("Metrics pushed in ~p second(s).~n", [Time]),
    stop = update_status(Reader, N+D0+S0),
    [ freya_tcp_client:stop(W) || W <- [Reader|Workers] ].

update_status(Reader, Threshold) ->
    {Drops, Saved, Connections} = get_status(Reader),
    io:format("Dropped ~w~nSaved ~w~nConnections ~w~n",
              [Drops, Saved, Connections]),
    io:format("----------------------------------------~n"),
    case ((Saved + Drops) >= Threshold) of
        true ->
            stop;
        _ ->
            timer:sleep(timer:seconds(1)),
            update_status(Reader, Threshold)
    end.

get_status(Reader) ->
    {ok, [Status]} = freya_tcp_client:status(Reader),
    Drops = kvlists:get_value(<<"drops">>, Status),
    Saved = kvlists:get_value(<<"saved">>, Status),
    Connections = kvlists:get_value(<<"connections">>, Status),
    {Drops, Saved, Connections}.

round_robin(Wrks, Ms) ->
    round_robin(Wrks, Ms, []).

round_robin(_, [], _) ->
    done;
round_robin([], Ms, WrksDone) ->
    round_robin(lists:reverse(WrksDone), Ms, []);
round_robin([CurrentWrk|Wrks], [{M,V}|Ms], WrksDone) ->
    ok = freya_tcp_client:put_metric(CurrentWrk, M, V),
    round_robin(Wrks, Ms, [CurrentWrk|WrksDone]).


metric() ->
    iolist_to_binary(base64:encode(crypto:strong_rand_bytes(16))).

