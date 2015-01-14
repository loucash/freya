-module(test_helpers).

-export([randomize/1]).
-export([keep_trying/4]).

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
