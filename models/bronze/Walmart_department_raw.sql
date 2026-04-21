{% set columns_table_department = {

'STORE':1,
'DEPT':2,
'DATE':3,
'WEEKLY_SALES':4,
'ISHOLIDAY':5

} %}

{{ config(

materialized='table',
alias='DEPARTMENT',

pre_hook=
macros_copy_csv(
'DEPARTMENT',
columns_table_department,
'.*department.*\\.csv')

) }}

select

STORE,
DEPT,
DATE,
WEEKLY_SALES,
ISHOLIDAY,
INSERT_DTS,
UPDATE_DTS,
SOURCE_FILE_NAME,
SOURCE_FILE_ROW_NUMBER

from {{ var('rawhist_db') }}.{{ var('wrk_schema') }}.DEPARTMENT