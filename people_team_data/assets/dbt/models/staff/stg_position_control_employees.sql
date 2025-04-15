{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select
        SAFE_CAST(employee_id as INT64) as employee_id, -- PII
        SAFE_CAST(employee_first_name as STRING) as employee_first_name, -- PII
        SAFE_CAST(employee_last_name as STRING) as employee_last_name, -- PII
        SAFE_CAST(employee_full_name as STRING) as employee_full_name, -- PII
        SAFE_CAST(employee_full as STRING) as employee_full, -- PII
        SAFE_CAST(employee_middle_name as STRING) as employee_middle_name, -- PII
        SAFE_CAST(employee_status as STRING) as employee_status,
        SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
        SAFE_CAST(_dlt_id as STRING) as _dlt_id
    from {{ source('raw_staff_data', 'raw_position_control_employees') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where SAFE_CAST(_dlt_load_id as STRING) > (select max(SAFE_CAST(_dlt_load_id as STRING)) from {{ this }})
    {% endif %}
)

select *
from raw