{{ config(
    materialized='table',
    schema='GOLD',
    alias='WALMART_DATE_DIM'
) }}

with unique_dates as (

    select distinct
        store_date,
        is_holiday,
        insert_dts,
        update_dts
    from {{ ref('department_clean') }}
    where store_date is not null

)

select
    row_number() over (order by store_date) as date_id,
    store_date,
    is_holiday,
    insert_dts as insert_date,
    update_dts as update_date
from unique_dates