-module(freya_tcp_codec).
-include("freya_tcp.hrl").
-include("freya_metrics.hrl").

-export([encode_version/0]).
-export([encode_status/0]).
-export([encode_put/1]).

-export([encode/1]).
-export([decode/1]).
-export([decode/2]).

encode_version() ->
    encode([?HDR, ?OPCODE_VERSION]).

encode_put({Name, Ts, Tags, V}) ->
    encode([?HDR, ?OPCODE_PUT, Name, Ts, Tags, V]).

encode_status() ->
    encode([?HDR, ?OPCODE_STATUS]).

-spec encode(term()) -> iodata().
encode(Term) ->
    msgpack:pack(Term, [{format, jsx}]).

decode(Stream) ->
    decode(Stream, undefined).

decode(Stream, undefined) ->
    do_decode(Stream);
decode(Stream, Buffered) ->
    do_decode(<<Buffered/binary, Stream/binary>>).

do_decode(Stream) ->
    do_decode(Stream, []).

do_decode(Stream, AlreadyDecoded) ->
    case msgpack:unpack_stream(Stream, [{format,jsx}]) of
        {error, incomplete} ->
            {incomplete, {Stream, lists:reverse(AlreadyDecoded)}};
        {Result, <<>>} ->
            {ok, lists:reverse([Result|AlreadyDecoded])};
        {Result, Next} ->
            do_decode(Next, [Result|AlreadyDecoded])
    end.
