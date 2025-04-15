{{ config(
    materialized='table',
    unique_key='assignment_id'
) }}

with raw as (

    select
        SAFE_CAST(assignment_id as STRING) as assignment_id,
        SAFE_CAST(assignment_count as INT64) as assignment_count,
        SAFE_CAST(employee_id as INT64) as employee_id, -- Potential PII
        SAFE_CAST(employee_count as INT64) as employee_count,
        SAFE_CAST(assignment_full as STRING) as assignment_full,
        SAFE_CAST(assignment_scale as STRING) as assignment_scale,
        SAFE_CAST(assignment_start_number as INT64) as assignment_start_number,
        SAFE_CAST(assignment_end_number as INT64) as assignment_end_number,
        SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
        SAFE_CAST(_dlt_id as STRING) as _dlt_id,
        SAFE_CAST(assignment_start as DATE) as assignment_start, -- Changed from PARSE_DATE
        SAFE_CAST(assignment_end as DATE) as assignment_end,     -- Changed from PARSE_DATE
        SAFE_CAST(assignment_status as STRING) as assignment_status,
        SAFE_CAST(position_id as STRING) as position_id,
        SAFE_CAST(position_full as STRING) as position_full,
        SAFE_CAST(employee_full as STRING) as employee_full, -- Potential PII
        SAFE_CAST(assignment_fte as DECIMAL) as assignment_fte,
        SAFE_CAST(assignment_calendar as DECIMAL) as assignment_calendar, -- Assuming DECIMAL, adjust if INT64
        SAFE_CAST(assignment_step as INT64) as assignment_step,
        SAFE_CAST(assignment_salary as DECIMAL) as assignment_salary,
        SAFE_CAST(assignment_ppp as DECIMAL) as assignment_ppp,
        SAFE_CAST(position_account as STRING) as position_account,
        SAFE_CAST(position_goal as STRING) as position_goal,
        SAFE_CAST(position_hr_division as STRING) as position_hr_division,
        SAFE_CAST(notes as STRING) as notes,
        SAFE_CAST(assignment_wage as DECIMAL) as assignment_wage,
        SAFE_CAST(assignment_reporting3 as STRING) as assignment_reporting3
    from {{ source('raw_staff_data', 'raw_position_control_assignments') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where SAFE_CAST(_dlt_load_id as STRING) > (select max(SAFE_CAST(_dlt_load_id as STRING)) from {{ this }})
    {% endif %}
)

select *
from raw