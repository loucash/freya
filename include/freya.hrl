-type metric_name()   :: binary().
-type milliseconds()  :: non_neg_integer().
-type unit()            :: seconds | minutes | hours | days | weeks.
-type aggregate()       :: avg | min | max | sum.
-type precision()       :: {non_neg_integer(), unit()}.

-record(data_point,{
          name      :: metric_name(),
          ts        :: milliseconds(),
          type      :: data_type(),
          tags = [] :: data_tags(),
          value     :: data_value(),
          meta      :: data_meta()
         }).
-type data_point()      :: #data_point{}.
-type data_tags()       :: proplists:proplist().
-type data_type()       :: long | double.
-type data_value()      :: any().
-type data_order()      :: asc | desc.
-type data_precision()  :: raw | {aggregate(), precision()}.
-type data_meta()       :: [{ttl, non_neg_integer() | infinity} |
                            {precision, data_precision()}].

-define(AGGREGATES, [avg, min, max, sum]).
-define(UNITS, [seconds, minutes, hours, days, weeks, months, years]).

% data model temporary constants
-define(RAW_ROW_WIDTH, {3, weeks}).

% erlcql_cluster pool
-type pool_name()   :: atom().
-define(CS_READ_POOL,       freya_read_pool).
-define(CS_WRITE_POOL,      freya_write_pool).

% poolboy writers
-define(CS_WRITERS_POOL,    freya_writers).
-define(CS_WRITERS_PUB,     freya_writers_pub).
