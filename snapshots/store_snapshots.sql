{% snapshot store_snapshots %}

{{
    config(
        target_database='WALMART_DB',
        target_schema='SNAPSHOTS',
        unique_key="store_id || '-' || dept_id",
        strategy='check',
        check_cols=['store_type', 'store_size']
    )
}}

select
    store_id,
    dept_id,
    store_type,
    store_size,
    insert_date,
    update_date
from {{ ref('store_dim') }}

{% endsnapshot %}