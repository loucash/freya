-module(freya_rest_dps).

-export([init/3]).
-export([rest_init/2]).
-export([content_types_provided/2]).
-export([allowed_methods/2]).
-export([resource_exists/2]).
-export([render_json/2]).
-export([render_msgpack/2]).

-include("freya.hrl").

-record(state, {
          ns :: metric_ns(),
          name :: metric_name(),
          search_opts :: proplists:proplist(),
          data_points :: list()
         }).

init(_Transport, _Req, []) ->
    {upgrade, protocol, cowboy_rest}.

rest_init(Req1, _Opts) ->
    {State, Req2} = init_state(Req1),
    {ok, Req2, State}.

init_state(R0) ->
    {Ns, R1} = cowboy_req:binding(ns, R0),
    {Name, R2} = cowboy_req:binding(metric_name, R1),
    {Qss, R3} = cowboy_req:qs_vals(R2),
    Pipe = [ {start_time , fun ts_or_relative/1},
             {end_time   , fun ts_or_relative/1},
             {align      , fun to_bool/1},
             {aggregate  , fun to_aggregator/1},
             {sampling   , fun to_relative_time/1},
             {tags       , fun to_kvlist/1},
             {order      , fun to_order/1} ], % TODO source
    Opts = lists:filtermap(
             fun({K,F}) ->
                     KBin = atom_to_binary(K, latin1),
                     case kvlists:get_value(KBin, Qss) of
                         undefined ->
                             false;
                         V ->
                             {true, {K, F(V)}}
                     end
             end, Pipe),
    AggDef = kvlists:get_values([aggregate,sampling], Opts),
    SearchOpts = case AggDef of
                     [undefined,undefined] ->
                         Opts;
                     [undefined,_] ->
                         error(bad_aggregate);
                     [_,undefined] ->
                         error(bad_aggregate);
                     [Aggr,Sampl] ->
                         O1 = kvlists:delete_value(aggregate, Opts),
                         O2 = kvlists:delete_value(sampling, O1),
                         [{aggregate,{Aggr,Sampl}}|O2]
                 end,
    {#state{ns=Ns, name=Name, search_opts=SearchOpts}, R3}.

resource_exists(Req, State=#state{ns=Ns, name=Name, search_opts=Opts}) ->
    SearchOpts = [{ns,Ns},{name,Name}|Opts],
    case freya_reader:search(SearchOpts) of
        {ok, Result} ->
            Dps = [ [freya_data_point:ts(Dp),
                     freya_data_point:value(Dp)] || Dp <- Result ],
            {true, Req, State#state{data_points=Dps}};
        {error, not_found} ->
            {false, Req, State}
    end.

-spec ts_or_relative(binary()) -> milliseconds().
ts_or_relative(Q0) ->
    Time = try
               binary_to_integer(Q0)
           catch error:badarg ->
                     to_relative_time(Q0)
           end,
    freya_utils:ms(Time).

-spec to_relative_time(binary()) -> precision().
to_relative_time(Q) ->
    [V, U] = binary:split(Q, <<",">>),
    {binary_to_integer(V), to_unit(U)}.

-spec to_kvlist(undefined | binary()) -> proplists:proplist().
to_kvlist(undefined) ->
    [];
to_kvlist(Q) when is_binary(Q) ->
    D = uridecode(Q),
    L = binary:split(D, [<<",">>,<<":">>], [global]),
    to_kvlist2(L).

to_kvlist2([]) -> [];
to_kvlist2([K,V|T]) -> [{uridecode(K),uridecode(V)}|to_kvlist2(T)].

uridecode(X) ->
    list_to_binary(http_uri:decode(binary_to_list(X))).

-spec to_order(binary()) -> data_order().
to_order(<<"asc">>) -> asc;
to_order(<<"desc">>) -> desc.

-spec to_bool(binary() | boolean()) ->
    boolean().
to_bool(B) when is_boolean(B) -> B;
to_bool(<<"true">>) -> true;
to_bool(<<"false">>) -> false.

-spec to_aggregator(binary()) -> aggregate().
to_aggregator(<<"avg">>) -> avg;
to_aggregator(<<"min">>) -> min;
to_aggregator(<<"max">>) -> max;
to_aggregator(<<"sum">>) -> sum.

-spec to_unit(binary()) -> unit().
to_unit(<<"seconds">>) -> seconds;
to_unit(<<"minutes">>) -> minutes;
to_unit(<<"hours">>) -> hours;
to_unit(<<"days">>) -> days;
to_unit(<<"weeks">>) -> weeks.

content_types_provided(Req, State) ->
    ContentTypes = [{<<"application/x-msgpack">>, render_msgpack},
                    {<<"application/json">>, render_json}],
    {ContentTypes, Req, State}.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"OPTIONS">>], Req, State}.

render_msgpack(Req, State=#state{data_points=Dps}) ->
    Resp = msgpack:pack(Dps, [{format, jsx}]),
    {Resp, Req, State}.

render_json(Req, State=#state{data_points=Dps}) ->
    Resp = jsx:encode(Dps),
    {Resp, Req, State}.
