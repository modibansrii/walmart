{% set columns_table_fact = {
    'STORE': 1,
    'DATE': 2,
    'TEMPERATURE': 3,
    'FUEL_PRICE': 4,
    'MARKDOWN1': 5,
    'MARKDOWN2': 6,
    'MARKDOWN3': 7,
    'MARKDOWN4': 8,
    'MARKDOWN5': 9,
    'CPI': 10,
    'UNEMPLOYMENT': 11,
    'ISHOLIDAY': 12
} %}

{{ config(
    materialized='table',
    transient=true,
    alias='FACT',
    schema='BRONZE',
    pre_hook=macros_copy_csv('FACT', columns_table_fact, '.*fact.*\\.csv')
) }}

SELECT
    STORE,
    DATE,
    TEMPERATURE,
    FUEL_PRICE,
    MARKDOWN1,
    MARKDOWN2,
    MARKDOWN3,
    MARKDOWN4,
    MARKDOWN5,
    CPI,
    UNEMPLOYMENT,
    ISHOLIDAY,
    INSERT_DTS,
    UPDATE_DTS,
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER
FROM {{ source('walmart_bronze_source', 'FACT') }}