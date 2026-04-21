select

    store as store_id,
    dept as dept_id,
    date as store_date,
    weekly_sales as store_weekly_sales,
    isholiday,
    insert_dts,
    update_dts,
    source_file_name,
    source_file_row_number

from {{ ref('Walmart_department_raw') }}