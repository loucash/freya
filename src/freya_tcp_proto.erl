-module(freya_tcp_proto).

-include("freya_tcp.hrl").
-include("freya.hrl").

-export([inbound/2]).

inbound([?HDR, ?OPCODE_STATUS], S) ->
    {ok,Drops} = freya_tcp_status:query(metrics_dropped),
    {ok,Saved} = freya_tcp_status:query(metrics_saved),
    {ok,Connections} = freya_tcp_status:query(connections),
    {reply, [{<<"drops">>, Drops},
             {<<"saved">>, Saved},
             {<<"connections">>, Connections}], S};

inbound([?HDR, ?OPCODE_VERSION], S) ->
    {reply, [{<<"version">>, S#proto.version}], S};


inbound([?HDR, ?OPCODE_PUT, Name, Ts, Tags, Value], S)
  when is_binary(Name)
       andalso is_integer(Ts)
       andalso is_list(Tags)
       andalso is_number(Value) ->
    Type = case Value of
               _ when is_float(Value) -> <<"kairos_double">>;
               _ when is_integer(Value) -> <<"kairos_long">>
           end,
    DP = #data_point{
            name = Name,
            ts = Ts,
            type = Type,
            tags = Tags,
            value = Value
           },
    Pub = S#proto.publisher,
    case freya_writer:save(Pub, DP) of
        ok ->
            freya_tcp_status:inc(metrics_saved);
        {error, no_capacity} ->
            freya_tcp_status:inc(metrics_dropped)
    end,
    {noreply, S};

inbound(Cmd, _S) ->
    {error, {unknown_command, Cmd}}.
