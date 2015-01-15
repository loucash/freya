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
