{{ config(
    materialized='table',
    schema='GOLD',
    alias='WALMART_FACT_TABLE'
) }}

with fact_table as (

    select
        s.store_id,
        s.dept_id,
        d.date_id,
        s.store_size,
        dc.store_weekly_sales,
        sm.fuel_price,
        sm.store_temperature,
        sm.unemployment,
        sm.cpi,
        sm.markdown1,
        sm.markdown2,
        sm.markdown3,
        sm.markdown4,
        sm.markdown5,
        s.insert_date,
        s.update_date,
        s.dbt_valid_from as vrsn_start_date,
        s.dbt_valid_to as vrsn_end_date

    from {{ ref('store_snapshots') }} s

    inner join {{ ref('department_clean') }} dc
        on s.store_id = dc.store_id
       and s.dept_id = dc.dept_id

    inner join {{ ref('store_metrics_clean') }} sm
        on dc.store_id = sm.store_id
       and dc.store_date = sm.store_date

    inner join {{ ref('date_dim') }} d
        on dc.store_date = d.store_date

)

select *
from fact_table