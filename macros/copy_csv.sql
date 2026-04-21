{% macro macros_copy_csv(table_name, columns_map, file_pattern) %}

    {% set ordered_columns = columns_map | dictsort(false, 'value') %}
    {% set target_columns = [] %}
    {% set select_list = [] %}

    {% for column_name, position in ordered_columns %}
        {% do target_columns.append('"' ~ column_name ~ '"') %}
        {% do select_list.append('$' ~ position ~ ' AS "' ~ column_name ~ '"') %}
    {% endfor %}

    {% do target_columns.append('"INSERT_DTS"') %}
    {% do target_columns.append('"UPDATE_DTS"') %}
    {% do target_columns.append('"SOURCE_FILE_NAME"') %}
    {% do target_columns.append('"SOURCE_FILE_ROW_NUMBER"') %}

    {% do select_list.append('CURRENT_TIMESTAMP() AS "INSERT_DTS"') %}
    {% do select_list.append('CURRENT_TIMESTAMP() AS "UPDATE_DTS"') %}
    {% do select_list.append('METADATA$FILENAME AS "SOURCE_FILE_NAME"') %}
    {% do select_list.append('METADATA$FILE_ROW_NUMBER AS "SOURCE_FILE_ROW_NUMBER"') %}

    {% set sql %}
        COPY INTO WALMART_DB.BRONZE.{{ table_name }}
        (
            {{ target_columns | join(',\n            ') }}
        )
        FROM (
            SELECT
                {{ select_list | join(',\n                ') }}
            FROM @WALMART_DB.BRONZE.WALMART_STAGE
        )
        PATTERN='{{ file_pattern }}'
        FILE_FORMAT = (FORMAT_NAME = WALMART_DB.BRONZE.WALMART_CSV_FORMAT)
        FORCE = TRUE;
    {% endset %}

    {{ sql }}

{% endmacro %}