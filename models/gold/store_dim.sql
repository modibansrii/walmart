{{ config(
    materialized='table',
    schema='GOLD',
    alias='WALMART_STORE_DIM'
) }}

with dept_data as (

    select distinct
        store_id,
        dept_id,
        insert_dts,
        update_dts
    from {{ ref('department_clean') }}

),

store_data as (

    select
        store_id,
        store_type,
        store_size
    from {{ ref('store_clean') }}

)

select
    d.store_id,
    d.dept_id,
    s.store_type,
    s.store_size,
    d.insert_dts as insert_date,
    d.update_dts as update_date
from dept_data d
left join store_data s
    on d.store_id = s.store_id