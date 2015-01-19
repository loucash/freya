-module(test_helpers).

-export([randomize/1]).
-export([keep_trying/4]).
-export([set_fixt_dir/2, load_fixt/2]).

-include_lib("common_test/include/ct.hrl").

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
