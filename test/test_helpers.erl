-module(test_helpers).

-export([setup_env/0]).
-export([randomize/1]).
-export([keep_trying/4, keep_trying_receive/3]).
-export([set_fixt_dir/2, load_fixt/2]).
-export([wait_until_node_ready/0]).

-include_lib("common_test/include/ct.hrl").

setup_env() ->
    _ = application:load(riak_core),
    ok = application:set_env(riak_core, handoff_port, 7333),
    ok = create_data_ring_dir().

randomize(S) ->
    Ts = tic:now_to_epoch_usecs(),
    <<_:16, TsBin/binary>> = list_to_binary(integer_to_list(Ts)),
    <<TsBin/binary, S/binary>>.

keep_trying(Match, F, Sleep, Tries) ->
    try
        case F() of
            Match ->
                ok;
            Unexpected ->
                throw({unexpected, Unexpected})
        end
    catch
        _:_=E ->
              timer:sleep(Sleep),
              case Tries-1 of
                  0 ->
                      error(E);
                  N ->
                      keep_trying(Match, F, Sleep, N)
              end
    end.

keep_trying_receive(Match, Sleep, Tries) ->
    keep_trying(Match, fun() -> receive Msg -> Msg after Sleep -> error end end,
                0, Tries).

set_fixt_dir(Test, Config) ->
    case code:which(Test) of
        Filename when is_list(Filename) ->
            CommonDir = filename:dirname(Filename) ++ "/fixtures/",
            [{common_data_dir, CommonDir}|Config]
    end.

load_fixt(Config, Filename) ->
    F = filename:join(?config(common_data_dir, Config), Filename),
    {ok, Fixture} = file:read_file(F),
    Fixture.

create_data_ring_dir() ->
    {ok, Dir} = file:get_cwd(),
    filelib:ensure_dir(filename:join([Dir, "data", "ring", "file"])),
    ok.

wait_until_node_ready() ->
    ok = keep_trying(true, fun() -> is_node_ready(node()) end, 200, 20).

is_node_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            case lists:member(Node, riak_core_ring:ready_members(Ring)) of
                true -> true;
                false -> {not_ready, Node}
            end;
        Other ->
            Other
    end.
