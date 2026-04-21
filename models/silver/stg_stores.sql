select

    store as store_id,
    type as store_type,
    size as store_size,
    insert_dts,
    update_dts,
    source_file_name,
    source_file_row_number

from {{ ref('Walmart_stores_raw') }}