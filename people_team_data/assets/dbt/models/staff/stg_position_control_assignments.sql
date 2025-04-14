{{ config(
    materialized='table',
    unique_key='assignment_id'
) }}

with raw as (

    select 
        trim(assignment_id) as assignment_id,
        cast(assignment_count as int) as assignment_count,
        cast(employee_id as int) as employee_id,
        cast(employee_count as int) as employee_count,
        trim(assignment_full) as assignment_full,
        trim(assignment_scale) as assignment_scale,
        cast(assignment_start_number as int) as assignment_start_number,
        cast(assignment_end_number as int) as assignment_end_number,
        trim(_dlt_load_id) as _dlt_load_id,
        trim(_dlt_id) as _dlt_id,
        cast(assignment_start as date) as assignment_start,
        cast(assignment_end as date) as assignment_end,
        trim(assignment_status) as assignment_status,
        trim(position_id) as position_id,
        trim(position_full) as position_full,
        trim(employee_full) as employee_full,
        cast(assignment_fte as decimal) as assignment_fte,
        cast(assignment_calendar as decimal) as assignment_calendar,
        cast(assignment_step as int) as assignment_step,
        cast(assignment_salary as decimal) as assignment_salary,
        cast(assignment_ppp as decimal) as assignment_ppp,
        trim(position_account) as position_account,
        trim(position_goal) as position_goal,
        trim(position_hr_division) as position_hr_division,
        trim(notes) as notes,
        cast(assignment_wage as decimal) as assignment_wage,
        trim(assignment_reporting3) as assignment_reporting3
    from {{ source('raw_staff_data', 'raw_position_control_assignments') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw