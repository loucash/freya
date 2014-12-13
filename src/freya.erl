-module(freya).
-export([start/0, stop/0]).
-export([get_env/1, get_env/2]).

-define(APP, ?MODULE).

start() ->
    application:ensure_all_started(?APP).

stop() ->
    application:stop(?APP).

get_env(Name) ->
    application:get_env(?APP, Name).

get_env(Name, Default) ->
    application:get_env(?APP, Name, Default).
