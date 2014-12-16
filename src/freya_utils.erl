-module(freya_utils).

-include("freya.hrl").

-export([floor/2, ceil/2, prev/2, next/2, ms/1]).
-export([pack_long/1, unpack_long/1]).

-spec floor(milliseconds(), precision()) ->
    milliseconds().
floor(MSec, Sample) ->
    MSec - (MSec rem ms(Sample)).

-spec ceil(milliseconds(), precision()) ->
    milliseconds().
ceil(MSec, Sample) ->
    next(floor(MSec, Sample), Sample).

-spec prev(milliseconds(), precision()) ->
    milliseconds().
prev(Ts, Sample) ->
    Ts - ms(Sample).

-spec next(milliseconds(), precision()) ->
    milliseconds().
next(Ts, Sample) ->
    Ts + ms(Sample).

-spec ms(precision()) -> milliseconds().
ms({N, weeks}) ->
    N * ms({7, days});
ms({N, days}) ->
    N * timer:hours(24);
ms({N, Tp}) ->
    timer:Tp(N).

% kairos uses it: https://developers.google.com/protocol-buffers/docs/encoding?csw=1#types
unpack_long(ValueBin) when is_binary(ValueBin) ->
    Value = unpack_unsigned_long(ValueBin),
    (Value bsr 1) bxor -(Value band 1).

pack_long(Value) when is_integer(Value) ->
    pack_unsigned_long((Value bsl 1) bxor  (Value bsr 63)).

pack_unsigned_long(Value) ->
    pack_unsigned_long(Value, <<>>).

pack_unsigned_long(Value, Acc) ->
    case Value band (bnot 16#7F) of
        0 ->
            <<Acc/binary, Value/integer>>;
        _ ->
            A = (Value band 16#7F) bor 16#80,
            pack_unsigned_long(Value bsr 7, <<Acc/binary, A/integer>>)
    end.

unpack_unsigned_long(Bin) ->
    unpack_unsigned_long(Bin, 0, 0).

unpack_unsigned_long(<<Byte:8/integer, Rest/binary>>, Shift, Result) when Shift < 64->
    Result2 = Result bor ((Byte band 16#7F) bsl Shift),
    case Byte band 16#80 of
        0 ->
            Result2;
        _ ->
            unpack_unsigned_long(Rest, Shift + 7, Result2)
    end.
