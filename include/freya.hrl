-type metric_name()   :: binary().
-type milliseconds()  :: non_neg_integer().
-type data_tags()     :: proplists:proplist().
-type data_type()     :: binary().
-type data_value()    :: any().
-type data_order()    :: asc | desc.

-record(data_point,{
          name      :: metric_name(),
          ts        :: milliseconds(),
          type      :: data_type(),
          tags = [] :: data_tags(),
          value     :: data_value()
         }).
-type data_point()  :: #data_point{}.

-define(DATA_TYPES,
        [<<"kairos_complex">>,
         <<"kairos_double">>,
         <<"kairos_legacy">>,
         <<"kairos_long">>,
         <<"kairos_string">>]).

-type unit()        :: seconds | minutes | hours | days | weeks.
-type precision()   :: {non_neg_integer(), unit()}.
-type aggregate()   :: avg | min | max | sum.

-define(AGGREGATES, [avg, min, max, sum]).
-define(UNITS, [seconds, minutes, hours, days, weeks, months, years]).

% data model temporary constants
-define(ROW_WIDTH, {3, weeks}).

% erlcql_cluster pool
-type pool_name()   :: atom().
-define(CS_READ_POOL,       freya_read_pool).
-define(CS_WRITE_POOL,      freya_write_pool).

% poolboy writers
-define(CS_WRITERS_POOL,    freya_writers).
-define(CS_WRITERS_PUB,     freya_writers_pub).

% prepared queries
-define(INSERT_DATA_POINT_TTL,  insert_data_point_ttl_q).
-define(INSERT_DATA_POINT,      insert_data_point_q).
-define(INSERT_ROW_INDEX,       insert_row_index_q).
-define(INSERT_STRING_INDEX,    insert_string_index_q).

-define(SELECT_ROWS_FROM_START_ASC,     select_start_row_index_asc_q).
-define(SELECT_ROWS_FROM_START_DESC,    select_start_row_index_desc_q).
-define(SELECT_ROWS_FROM_START(Order),
        (fun(asc)  -> ?SELECT_ROWS_FROM_START_ASC;
            (desc) -> ?SELECT_ROWS_FROM_START_DESC end)(Order)).
-define(SELECT_ROWS_IN_RANGE_ASC,       select_range_row_index_asc_q).
-define(SELECT_ROWS_IN_RANGE_DESC,      select_range_row_index_desc_q).
-define(SELECT_ROWS_IN_RANGE(Order),
        (fun(asc)  -> ?SELECT_ROWS_IN_RANGE_ASC;
            (desc) -> ?SELECT_ROWS_IN_RANGE_DESC end)(Order)).
-define(SELECT_DATA_FROM_START_ASC,     select_start_data_point_asc_q).
-define(SELECT_DATA_FROM_START_DESC,    select_start_data_point_desc_q).
-define(SELECT_DATA_FROM_START(Order),
        (fun(asc)  -> ?SELECT_DATA_FROM_START_ASC;
            (desc) -> ?SELECT_DATA_FROM_START_DESC end)(Order)).
-define(SELECT_DATA_IN_RANGE_ASC,       select_range_data_point_asc_q).
-define(SELECT_DATA_IN_RANGE_DESC,      select_range_data_point_desc_q).
-define(SELECT_DATA_IN_RANGE(Order),
        (fun(asc)  -> ?SELECT_DATA_IN_RANGE_ASC;
            (desc) -> ?SELECT_DATA_IN_RANGE_DESC end)(Order)).

% string index keys
-define(ROW_KEY_METRIC_NAMES,   <<"metric_names">>).
-define(ROW_KEY_TAG_NAMES,      <<"tag_names">>).
-define(ROW_KEY_TAG_VALUES,     <<"tag_values">>).
