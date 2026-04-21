{% set columns_table_stores = {

'STORE':1,
'TYPE':2,
'SIZE':3

} %}

{{ config(

materialized='table',
alias='STORES',

pre_hook=
macros_copy_csv(
'STORES',
columns_table_stores,
'.*stores.*\\.csv')

) }}

select *

from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.STORES