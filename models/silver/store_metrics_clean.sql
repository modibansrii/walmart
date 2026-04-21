{{ config(materialized='table', schema='SILVER') }}

select
    store::int as store_id,
    try_to_date(date, 'MM/DD/YYYY') as store_date,

    temperature::decimal(10,2) as store_temperature,
    fuel_price::decimal(10,3) as fuel_price,

    markdown1::decimal(18,2) as markdown1,
    markdown2::decimal(18,2) as markdown2,
    markdown3::decimal(18,2) as markdown3,
    markdown4::decimal(18,2) as markdown4,
    markdown5::decimal(18,2) as markdown5,

    cpi::decimal(18,7) as cpi,
    unemployment::decimal(18,3) as unemployment,

    case
        when lower(isholiday) in ('true', '1') then true
        else false
    end as is_holiday,

    insert_dts,
    update_dts,
    source_file_name,
    source_file_row_number

from {{ ref('Walmart_fact_raw') }}