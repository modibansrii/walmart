{% set columns_table_department = {
    'STORE': 1,
    'DEPT': 2,
    'DATE': 3,
    'WEEKLY_SALES': 4,
    'ISHOLIDAY': 5
} %}

{{ config(
    materialized='table',
    transient=true,
    alias='DEPARTMENT',
    schema='BRONZE',
    pre_hook=macros_copy_csv('DEPARTMENT', columns_table_department, '.*department.*\\.csv')
) }}

SELECT
    STORE,
    DEPT,
    DATE,
    WEEKLY_SALES,
    ISHOLIDAY,
    INSERT_DTS,
    UPDATE_DTS,
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER
FROM {{ source('walmart_bronze_source', 'DEPARTMENT') }}