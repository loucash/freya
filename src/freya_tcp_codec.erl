-module(freya_tcp_codec).
-include("freya_tcp.hrl").

-export([encode_version/0]).
-export([encode_status/0]).
-export([encode_put/1]).

-export([encode/1]).
-export([decode/1]).

-export([encode_wire/1]).


encode_version() ->
    encode([?HDR, ?OPCODE_VERSION]).

encode_put({Name, Ts, Tags, V}) ->
    encode([?HDR, ?OPCODE_PUT, Name, Ts, Tags, V]).

encode_status() ->
    encode([?HDR, ?OPCODE_STATUS]).


-spec encode(term()) -> iodata().
encode(Term) ->
    encode_wire(msgpack:pack(Term, [{format, jsx}])).


-spec encode_wire(binary() | iolist()) -> binary().
encode_wire(Packet) when is_list(Packet) ->
    encode_wire(iolist_to_binary(Packet));
encode_wire(<<Packet/binary>>) ->
    Packet.


-spec decode(binary()) -> {ok, [term()]}
                          | {error, term()}.
decode(Stream) ->
    N = <<"freya.codec.decode_stream">>,
    T = quintana:begin_timed(N),
    R = decode(Stream, []),
    quintana:notify_timed(T),
    R.

decode(<<>>, Buffer) ->
    {ok, lists:reverse(Buffer)};
decode(Stream, Buffer) ->
    case msgpack:unpack_stream(Stream, [{format, jsx}]) of
        {error, R} -> error({?MODULE, R});
        {Packet, Rest} ->
            decode(Rest, [Packet|Buffer])
    end.
