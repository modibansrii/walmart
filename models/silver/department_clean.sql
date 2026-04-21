{{ config(materialized='table', schema='SILVER') }}

select
    store::int as store_id,
    dept::int as dept_id,
    try_to_date(date, 'MM/DD/YYYY') as store_date,
    weekly_sales::decimal(18,2) as store_weekly_sales,

    case
        when lower(isholiday) in ('true', '1') then true
        else false
    end as is_holiday,

    insert_dts,
    update_dts,
    source_file_name,
    source_file_row_number

from {{ ref('Walmart_department_raw') }}