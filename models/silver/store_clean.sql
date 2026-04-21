{{ config(materialized='table', schema='SILVER') }}

select
    store::int as store_id,
    trim(type) as store_type,
    size::int as store_size,

    insert_dts,
    update_dts,
    source_file_name,
    source_file_row_number

from {{ ref('Walmart_stores_raw') }}