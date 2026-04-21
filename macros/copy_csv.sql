{% macro macros_copy_csv(table_nm, column_list, file_pattern) %}

{% set sql %}

delete from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }};

copy into {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.{{ table_nm }}
from (

    select

    {%- for col, idx in column_list.items() %}

        ${{ idx }} as {{ col }}{{ "," if not loop.last }}

    {%- endfor %}

        , current_timestamp() as INSERT_DTS
        , current_timestamp() as UPDATE_DTS
        , metadata$filename as SOURCE_FILE_NAME
        , metadata$file_row_number as SOURCE_FILE_ROW_NUMBER

    from @{{ var('stage_name') }}

)

file_format = (format_name='{{ var("file_format_csv") }}')

pattern='{{ file_pattern }}'

purge={{ var('purge_status') }}

force=true;

{% endset %}

{% do run_query(sql) %}

{% endmacro %}