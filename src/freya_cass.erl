-module(freya_cass).

-include("freya.hrl").

-export([statements/0]).
-export([save/1]).

statements() ->
    [
     {?INSERT_DATA_POINT_TTL,
      <<"INSERT INTO data_points (key, column1, value) "
        "VALUES (?, ?, ?) USING TTL ?;">>},
     {?INSERT_DATA_POINT,
      <<"INSERT INTO data_points (key, column1, value) "
        "VALUES (?, ?, ?);">>},
     {?INSERT_ROW_INDEX,
      <<"INSERT INTO row_key_index (key, column1, value) "
        "VALUES (?, ?, ?);">>},
     {?INSERT_STRING_INDEX,
      <<"INSERT INTO string_index (key, column1, value) "
        "VALUES (?, ?, ?);">>}
    ].

save(#data_point{}=DP) ->
    {ok, {Row, Column, Value}} = freya_blobs:encode(DP),
    Q1 = {?INSERT_DATA_POINT, [Row, Column, Value]},
    Q2 = {?INSERT_ROW_INDEX, [DP#data_point.name, Row, <<0>>]},
    [Q1, Q2].
