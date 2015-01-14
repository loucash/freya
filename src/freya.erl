-module(freya).
-export([start/0, stop/0]).
-export([get_env/1, get_env/2]).
-export([version/0]).

-define(APP, ?MODULE).

start() ->
    application:ensure_all_started(?APP).

stop() ->
    application:stop(?APP).

get_env(Name) ->
    application:get_env(?APP, Name).

get_env(Name, Default) ->
    application:get_env(?APP, Name, Default).

version() ->
    {ok, Vsn} = application:get_key(freya, vsn),
    CompileTime = kvlists:get_path([compile, time], freya_app:module_info()),
    {Y,M,D,Hr,Min,Sec} = CompileTime,
    BuildDate = tic:datetime_to_iso8601({{Y,M,D},{Hr,Min,Sec}}),
    Version = [atom_to_list(?MODULE), " ", Vsn, " build: ", BuildDate],
    {ok, iolist_to_binary(Version)}.
