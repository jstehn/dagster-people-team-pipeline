{{ config(
    materialized='table',
    unique_key='position_id'
) }}

with raw as (

    select
        SAFE_CAST(position_id as STRING) as position_id,
        SAFE_CAST(position_unique as BOOLEAN) as position_unique,
        SAFE_CAST(assignment_count as INT64) as assignment_count,
        SAFE_CAST(position_start as DATE) as position_start, -- Changed from PARSE_DATE
        SAFE_CAST(position_status as STRING) as position_status,
        SAFE_CAST(position_hr_department as STRING) as position_hr_department,
        SAFE_CAST(position_hr_division as STRING) as position_hr_division,
        SAFE_CAST(position_hr_sub_department as STRING) as position_hr_sub_department,
        SAFE_CAST(position_code as STRING) as position_code,
        SAFE_CAST(position_count as STRING) as position_count, -- Assuming STRING, adjust if numeric
        SAFE_CAST(position_name as STRING) as position_name,
        SAFE_CAST(position_account as STRING) as position_account,
        SAFE_CAST(position_goal as STRING) as position_goal,
        SAFE_CAST(position_full as STRING) as position_full,
        SAFE_CAST(position_credential as STRING) as position_credential,
        SAFE_CAST(position_start_number as INT64) as position_start_number,
        SAFE_CAST(position_end_number as INT64) as position_end_number,
        SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
        SAFE_CAST(_dlt_id as STRING) as _dlt_id,
        SAFE_CAST(notes as STRING) as notes
    from {{ source('raw_staff_data', 'raw_position_control_positions') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where SAFE_CAST(_dlt_load_id as STRING) > (select max(SAFE_CAST(_dlt_load_id as STRING)) from {{ this }})
    {% endif %}
)

select *
from raw