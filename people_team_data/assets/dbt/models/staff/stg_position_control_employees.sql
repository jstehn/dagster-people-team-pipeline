{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select 
        cast(employee_code as int) as employee_id,
        trim(legal_firstname) as employee_first_name,
        trim(legal_lastname) as employee_last_name,
        trim(employee_first_name) as employee_full_name,
        trim(employee_full) as employee_full,
        trim(employee_middle_name) as employee_middle_name,
        trim(employee_status) as employee_status,
        _dlt_load_id,
        _dlt_id
    from {{ source('raw_staff_data', 'raw_position_control_employees') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw