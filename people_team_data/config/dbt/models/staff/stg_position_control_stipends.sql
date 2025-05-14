{{ config(
    materialized='table',
    unique_key='stipend_id'
) }}

with raw as (

    select
        SAFE_CAST(stipend_id as STRING) as stipend_id,
        SAFE_CAST(employee_id as INT64) as employee_id, -- PII
        SAFE_CAST(employee_name as STRING) as employee_name, -- PII
        SAFE_CAST(stipend_start_date as DATE) as stipend_start_date, -- Changed from PARSE_DATE
        SAFE_CAST(stipend_end_date as DATE) as stipend_end_date,     -- Changed from PARSE_DATE
        SAFE_CAST(stipend_status as STRING) as stipend_status,
        SAFE_CAST(stipend_category as STRING) as stipend_category,
        SAFE_CAST(stipend_description as STRING) as stipend_description,
        SAFE_CAST(stipend_amount as DECIMAL) as stipend_amount,
        SAFE_CAST(stipend_full_account as STRING) as stipend_full_account,
        SAFE_CAST(stipend_account as STRING) as stipend_account,
        SAFE_CAST(stipend_full_goal as STRING) as stipend_full_goal,
        SAFE_CAST(stipend_goal as STRING) as stipend_goal,
        SAFE_CAST(stipend_location as STRING) as stipend_location,
        SAFE_CAST(stipend_full_resource as STRING) as stipend_full_resource,
        SAFE_CAST(stipend_resource as STRING) as stipend_resource,
        SAFE_CAST(stipend_start_number as INT64) as stipend_start_number,
        SAFE_CAST(stipend_end_number as INT64) as stipend_end_number,
        SAFE_CAST(ytd_count as INT64) as ytd_count,
        SAFE_CAST(ytd_total as DECIMAL) as ytd_total, -- Assuming DECIMAL, adjust if needed
        SAFE_CAST(future_count as INT64) as future_count,
        SAFE_CAST(future_total as DECIMAL) as future_total, -- Assuming DECIMAL, adjust if needed
        SAFE_CAST(stipend_count as INT64) as stipend_count,
        SAFE_CAST(stipend_total as DECIMAL) as stipend_total, -- Assuming DECIMAL, adjust if needed
        SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
        SAFE_CAST(_dlt_id as STRING) as _dlt_id,
        SAFE_CAST(employee_id__v_text as STRING) as employee_id__v_text, -- PII
        SAFE_CAST(notes as STRING) as notes
    from {{ source('raw_staff_data', 'raw_position_control_stipends') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where SAFE_CAST(_dlt_load_id as STRING) > (select max(SAFE_CAST(_dlt_load_id as STRING)) from {{ this }})
    {% endif %}
)

select *
from raw