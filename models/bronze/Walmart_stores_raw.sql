/* Walmart_stores_raw.sql */

{% set columns_table_stores = {
    'STORE': 1,
    'TYPE': 2,
    'SIZE': 3
} %}

{{ config(
    materialized='table',
    transient=true,
    alias='STORES',
    schema='BRONZE',
    pre_hook=macros_copy_csv('STORES', columns_table_stores, '.*stores.*\\.csv')
) }}

SELECT
    STORE,
    TYPE,
    SIZE,
    INSERT_DTS,
    UPDATE_DTS,
    SOURCE_FILE_NAME,
    SOURCE_FILE_ROW_NUMBER
FROM {{ source('walmart_bronze_source', 'STORES') }}