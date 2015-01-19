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
    spam(random, WrkCount, N).

spam(MetricName, WrkCount, N) ->
    application:ensure_all_started(folsomite),
    application:ensure_all_started(quintana),
    application:load(freya),
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
    Metrics = [{metric(MetricName), random:uniform()} || _ <- lists:seq(1, N)],
    io:format("Metrics generated.~n"),
    {Us, done} = timer:tc(fun() -> round_robin(Workers, Metrics) end),
    Time = Us * 0.000001,
    quintana:notify_timed(T),
    io:format("Metrics pushed. Veryifing..."),
    {SavedUs, {stop, {Saved, Drops, Sleeps}}}
        = timer:tc(fun() -> update_status(Reader, N+D0+S0) end),
    SavedTime = (SavedUs * 0.000001) - (Sleeps * 0.001) ,
    {ok,WritersSize} = application:get_env(freya, writers_size),
    {ok,WriteDelay} = application:get_env(freya, write_delay),
    {ok,WriteBatchSize} = application:get_env(freya, write_batch_size),
    Efficiency = (100*Saved) / (Saved+Drops),
    io:format("Summary:~n--------~n"),
    io:format("Writers size: ~w~n", [WritersSize]),
    io:format("Write delay: ~w~n", [WriteDelay]),
    io:format("Write batch size: ~w~n", [WriteBatchSize]),
    io:format("Efficiency: ~p%~n", [Efficiency]),
    io:format("Pushed: ~p second(s)~n", [Time]),
    io:format("Verified: ~p second(s)~n", [SavedTime]),
    [ freya_tcp_client:stop(W) || W <- [Reader|Workers] ].

update_status(Reader, Threshold) ->
    update_status(Reader, Threshold, 0).

update_status(Reader, Threshold, Sleeps) ->
    {Drops, Saved, Connections} = get_status(Reader),
    case ((Saved + Drops) >= Threshold) of
        true ->
            io:format("Dropped ~w~nSaved ~w~nConnections ~w~n",
                      [Drops, Saved, Connections]),
            {stop, {Saved, Drops, Sleeps}};
        _ ->
            timer:sleep(1),
            update_status(Reader, Threshold, Sleeps+1)
    end.

get_status(Reader) ->
    {ok, [Status]} = freya_tcp_client:status(Reader),
    Drops = kvlists:get_value(<<"drops">>, Status),
    Saved = kvlists:get_value(<<"saved">>, Status),
    Connections = kvlists:get_value(<<"connections">>, Status),
    {Drops, Saved, Connections}.

round_robin(Wrks, Ms) ->
    TS = tic:now_to_epoch_msecs(),
    round_robin(Wrks, Ms, [], TS).

round_robin(_, [], _, _) ->
    done;
round_robin([], Ms, WrksDone, TS) ->
    round_robin(lists:reverse(WrksDone), Ms, [], TS);
round_robin([CurrentWrk|Wrks], [{M,V}|Ms]=MX, WrksDone, TS) ->
    ok = freya_tcp_client:put_metric(CurrentWrk, M, TS, V),
    NewTS = TS - (timer:seconds(length(MX))),
    round_robin(Wrks, Ms, [CurrentWrk|WrksDone], NewTS).

metric(random) ->
    iolist_to_binary(["metric",
                      base64:encode(crypto:strong_rand_bytes(random:uniform(16)))]);
metric(Name) ->
    Name.
