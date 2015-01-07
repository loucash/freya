-module(freya_tcp_proto).

-include("freya_tcp.hrl").

-export([inbound/2]).


inbound(version, S) ->
    {reply, S#proto.version, S};

inbound(Cmd, _S) ->
    {error, {unknown_command, Cmd}}.
