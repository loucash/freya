-module(freya_cass).

-include("freya.hrl").

-export([statements/0]).
-export([save_data_point_queries/1]).

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

save_data_point_queries(#data_point{}=DP) ->
    {ok, {Row, Column, Value}} = freya_blobs:encode(DP),
    [{?INSERT_DATA_POINT, [Row, Column, Value]},
     {?INSERT_ROW_INDEX, [DP#data_point.name, Row, <<0>>]},
     {?INSERT_STRING_INDEX, [?ROW_KEY_METRIC_NAMES, DP#data_point.name, <<0>>]}]
    ++ lists:flatmap(
         fun(TagName, TagValue) ->
            [{?INSERT_STRING_INDEX, [?ROW_KEY_TAG_NAMES, TagName, <<0>>]},
             {?INSERT_STRING_INDEX, [?ROW_KEY_TAG_VALUES, TagValue, <<0>>]}]
         end, DP#data_point.tags).
