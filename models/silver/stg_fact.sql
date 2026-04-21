select

    store as store_id,
    date as store_date,
    temperature as store_temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    unemployment,
    isholiday,
    insert_dts,
    update_dts,
    source_file_name,
    source_file_row_number

from {{ ref('Walmart_fact_raw') }}