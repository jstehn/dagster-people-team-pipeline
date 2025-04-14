{{ config(
    materialized='table',
    unique_key='position_id'
) }}

with raw as (

    select 
        trim(position_id) as position_id,
        cast(position_unique as boolean) as position_unique,
        cast(assignment_count as int) as assignment_count,
        cast(position_start as date) as position_start,
        trim(position_status) as position_status,
        trim(position_hr_department) as position_hr_department,
        trim(position_hr_division) as position_hr_division,
        trim(position_hr_sub_department) as position_hr_sub_department,
        trim(position_code) as position_code,
        trim(position_count) as position_count,
        trim(position_name) as position_name,
        trim(position_account) as position_account,
        trim(position_goal) as position_goal,
        trim(position_full) as position_full,
        trim(position_credential) as position_credential,
        cast(position_start_number as int) as position_start_number,
        cast(position_end_number as int) as position_end_number,
        trim(_dlt_load_id) as _dlt_load_id,
        trim(_dlt_id) as _dlt_id,
        trim(notes) as notes
    from {{ source('raw_staff_data', 'raw_position_control_positions') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw