-type milliseconds()        :: non_neg_integer().
-type data_point_tags()     :: proplists:proplist().
-type data_point_type()     :: binary().
-type data_point_value()    :: number().

-record(data_point,{
          name      :: string() | binary(),
          ts        :: milliseconds(),
          type      :: data_point_type(),
          tags = [] :: data_point_tags(),
          value     :: data_point_value(),
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

-define(CS_READ_POOL, freya_read_pool).
-define(CS_WRITE_POOL, freya_write_pool).

-define(ROW_WIDTH, {3, weeks}).
