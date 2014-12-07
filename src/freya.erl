-module(freya).
-export([start/0, stop/0]).

start() ->
    application:ensure_all_started(freya).

stop() ->
    application:stop(freya).
