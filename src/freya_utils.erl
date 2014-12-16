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
unpack_long(Value) when is_integer(Value) ->
    (Value bsr 1) bxor -(Value band 1).

pack_long(Value) when is_integer(Value) ->
    (Value bsl 1) bxor  (Value bsr 63).
