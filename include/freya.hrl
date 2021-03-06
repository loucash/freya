-type metric_ns()       :: binary().
-type metric_name()     :: binary().
-type metric()          :: {metric_ns(), metric_name()}
                         | metric_name().
-type milliseconds()    :: non_neg_integer().
-type ttl()             :: non_neg_integer().
-type unit()            :: seconds | minutes | hours | days | weeks.
-type aggregate()       :: avg | min | max | sum.
-type precision()       :: {non_neg_integer(), unit()} | non_neg_integer().

-define(DEFAULT_NS, <<"default">>).

-record(data_point,{
          ns = ?DEFAULT_NS :: metric_ns(),
          name             :: metric_name(),
          ts               :: milliseconds(),
          type             :: data_type(),
          tags = []        :: data_tags(),
          value            :: data_value(),
          meta             :: data_meta()
         }).

-type data_point()      :: #data_point{}.
-type data_tags()       :: proplists:proplist().
-type data_type()       :: long | double.
-type data_value()      :: any().
-type data_order()      :: asc | desc.
-type data_precision()  :: raw | {aggregate(), precision()}.
-type data_meta()       :: [{ttl, non_neg_integer() | infinity} |
                            {precision, data_precision()}].
-type data_key()        :: tuple().

-define(AGGREGATES, [avg, min, max, sum]).
-define(UNITS, [seconds, minutes, hours, days, weeks, months, years]).

% cassandra limit is 2 billions, we set max size to 1.9 billion
-define(MAX_ROW_WIDTH, 1900000000).
% cassandra max ttl: 20 years
-define(MAX_TTL, 630720000).
% maximum time period per row: 20 years
-define(MAX_WEEKS, 1042).

% erlcql_cluster pool
-type pool_name()   :: atom().
-define(CS_READ_POOL,       freya_read_pool).
-define(CS_WRITE_POOL,      freya_write_pool).

% poolboy writers
-define(CS_WRITERS_POOL,    freya_writers).
-define(CS_WRITERS_PUB,     freya_writers_pub).

% string index keys
-define(ROW_KEY_METRIC_NAMES(Ns), <<"_ns_", Ns/binary>>).
-define(ROW_KEY_TAG_NAMES,        <<"_tag_names">>).
-define(ROW_KEY_TAG_VALUES,       <<"_tag_values">>).
-define(ROW_KEY_NAMESPACES,       <<"_namespaces">>).
