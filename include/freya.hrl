-type metric_name()   :: binary().
-type milliseconds()  :: non_neg_integer().
-type data_tags()     :: proplists:proplist().
-type data_type()     :: binary().
-type data_value()    :: any().

-record(data_point,{
          name      :: metric_name(),
          ts        :: milliseconds(),
          type      :: data_type(),
          tags = [] :: data_tags(),
          value     :: data_value(),
          row_time  :: milliseconds()
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

-define(SELECT_ROWS_FROM_START, select_start_row_index_q).
-define(SELECT_ROWS_IN_RANGE,   select_range_row_index_q).
-define(SELECT_DATA_FROM_START, select_start_data_point_q).
-define(SELECT_DATA_IN_RANGE,   select_range_data_point_q).

% string index keys
-define(ROW_KEY_METRIC_NAMES,   <<"metric_names">>).
-define(ROW_KEY_TAG_NAMES,      <<"tag_names">>).
-define(ROW_KEY_TAG_VALUES,     <<"tag_values">>).
