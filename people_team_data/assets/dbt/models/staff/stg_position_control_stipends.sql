{{ config(
    materialized='table',
    unique_key='stipend_id'
) }}

with raw as (

    select 
        trim(stipend_id) as stipend_id,
        safe_cast(employee_id as int) as employee_id,
        trim(employee_name) as employee_name,
        safe_cast(stipend_start_date as date) as stipend_start_date,
        safe_cast(stipend_end_date as date) as stipend_end_date,
        trim(stipend_status) as stipend_status,
        trim(stipend_category) as stipend_category,
        trim(stipend_description) as stipend_description,
        safe_cast(stipend_amount as decimal) as stipend_amount,
        trim(stipend_full_account) as stipend_full_account,
        trim(stipend_account) as stipend_account,
        trim(stipend_full_goal) as stipend_full_goal,
        trim(stipend_goal) as stipend_goal,
        trim(stipend_location) as stipend_location,
        trim(stipend_full_resource) as stipend_full_resource,
        trim(stipend_resource) as stipend_resource,
        safe_cast(stipend_start_number as int) as stipend_start_number,
        safe_cast(stipend_end_number as int) as stipend_end_number,
        safe_cast(ytd_count as int) as ytd_count,
        trim(ytd_total) as ytd_total,
        safe_cast(future_count as int) as future_count,
        trim(future_total) as future_total,
        safe_cast(stipend_count as int) as stipend_count,
        trim(stipend_total) as stipend_total,
        trim(_dlt_load_id) as _dlt_load_id,
        trim(_dlt_id) as _dlt_id,
        trim(employee_id__v_text) as employee_id__v_text,
        trim(notes) as notes
    from {{ source('raw_staff_data', 'raw_position_control_stipends') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw