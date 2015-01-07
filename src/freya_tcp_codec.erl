-module(freya_tcp_codec).

-export([encode/1]).
-export([decode/1]).

-spec encode(term()) -> binary().
encode(Term) ->
    term_to_binary(Term).

-spec decode(binary()) -> term().
decode(Data) ->
    binary_to_term(Data).
