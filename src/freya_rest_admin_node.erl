-module(freya_rest_admin_node).

-export([init/3]).
-export([rest_init/2]).
-export([content_types_accepted/2, content_types_provided/2]).
-export([allowed_methods/2]).
-export([malformed_request/2]).
-export([content_acceptor/2, content_provider/2]).

-record(state, {
          cmd       :: binary(),
          method    :: binary(),
          node      :: binary()
         }).

init(_Transport, _Req, _Opts) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(R0, _Opts) ->
    {Cmd, R1} = cowboy_req:binding(cmd, R0),
    {Method, R2} = cowboy_req:method(R1),
    {ok, R2, #state{cmd=Cmd, method=Method}}.

allowed_methods(Req, #state{cmd = <<"join">>}=State) ->
    {[<<"POST">>], Req, State};
allowed_methods(Req, #state{cmd = <<"leave">>}=State) ->
    {[<<"POST">>], Req, State};
allowed_methods(Req, #state{cmd = <<"status">>}=State) ->
    {[<<"GET">>], Req, State}.

malformed_request(R0, #state{cmd = <<"join">>} = State) ->
    case extract_join_params(R0) of
        {ok, Node} ->
            {false, R0, State#state{node=Node}};
        {error, Reason} ->
            R1 = render_error(Reason, R0),
            {true, R1, State}
    end;
malformed_request(Req, State) ->
    {false, Req, State}.

content_types_provided(Req, State) ->
    ContentTypes = [{{<<"application">>, <<"json">>, '*'}, content_provider}],
    {ContentTypes, Req, State}.

content_types_accepted(Req, State) ->
    ContentTypes = [{{<<"application">>, <<"json">>, '*'}, content_acceptor}],
    {ContentTypes, Req, State}.

content_acceptor(R0, #state{cmd = <<"join">>,  method = <<"POST">>, node=Node}=State) ->
    case riak_core:join(Node) of
        ok ->
            {true, R0, State};
        {error, Reason} ->
            R1 = render_error(Reason, R0),
            {ok, R2} = cowboy_req:reply(503, R1),
            {halt, R2, State}
    end;
content_acceptor(R0, #state{cmd = <<"leave">>, method = <<"POST">>}=State) ->
    case riak_core:leave() of
        ok ->
            {true, R0, State};
        {error, Reason} ->
            R1 = render_error(Reason, R0),
            {ok, R2} = cowboy_req:reply(503, R1),
            {halt, R2, State}
    end;
content_acceptor(Req, State) ->
    {false, Req, State}.

content_provider(Req, #state{cmd = <<"status">>, method = <<"GET">>}=State) ->
    Response = case freya_utils:ring_member_status() of
                   {ok, {Status, PercentDone}} ->
                       [{<<"status">>, Status}, {<<"progress">>, PercentDone}];
                   {ok, Status} ->
                       [{<<"status">>, Status}];
                   {error, invalid} ->
                       [{<<"status">>, invalid}]
               end,
    {jsx:encode(Response), Req, State}.

extract_join_params(Req) ->
    Fns = [
            fun get_body/1,
            fun decode_body/1,
            fun validate_body/1
          ],
    hope_result:pipe(Fns, Req).

get_body(Req) ->
    case cowboy_req:body(Req) of
        {ok, Body, _} ->
            {ok, Body};
        {error, _} = Error ->
            Error
    end.

decode_body(Body) ->
    try
        Json = jsx:decode(Body),
        {ok, Json}
    catch
        error:badarg ->
            {error, invalid_json}
    end.

validate_body(Json) ->
    case Json of
        [{<<"node">>, Node}] ->
            {ok, binary_to_list(Node)};
        _ ->
            {error, invalid_json}
    end.

render_error(Code, Req0) ->
    Error = jsx:encode([{<<"error">>, Code}]),
    Req1 = cowboy_req:set_resp_header(
             <<"content-type">>, <<"application/json">>, Req0),
    cowboy_req:set_resp_body(Error, Req1).
