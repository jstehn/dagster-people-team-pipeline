{{ config(
    materialized='table',
    unique_key='adjustment_id'
) }}

with raw as (

    select
        -- Assuming these are date or timestamp strings, adjust format if needed
        SAFE_CAST(adjustment_begin_payroll as STRING) as adjustment_begin_payroll, -- Or SAFE.PARSE_DATE/TIMESTAMP
        SAFE_CAST(adjustment_end_payroll as STRING) as adjustment_end_payroll,   -- Or SAFE.PARSE_DATE/TIMESTAMP
        SAFE_CAST(adjustment_id as STRING) as adjustment_id,
        SAFE_CAST(assignment_full as STRING) as assignment_full,
        SAFE_CAST(adjustment_status as STRING) as adjustment_status,
        SAFE_CAST(adjustment_category as STRING) as adjustment_category,
        SAFE_CAST(adjustment_description as STRING) as adjustment_description,
        SAFE_CAST(adjustment_salary as DECIMAL) as adjustment_salary,
        SAFE_CAST(adjustmentxx as INT64) as adjustmentxx, -- Assuming 'adjustmentxx' is the correct column name
        SAFE_CAST(adjustment_ppp as DECIMAL) as adjustment_ppp,
        SAFE_CAST(assignment_location as STRING) as assignment_location,
        SAFE_CAST(adjustment_count as INT64) as adjustment_count,
        SAFE_CAST(adjustment_total as DECIMAL) as adjustment_total,
        SAFE_CAST(notes as STRING) as notes,
        SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id, -- Cast dlt columns to STRING
        SAFE_CAST(_dlt_id as STRING) as _dlt_id
    from {{ source('raw_staff_data', 'raw_position_control_adjustments') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      -- Ensure the column used for filtering exists and has a comparable type
      where SAFE_CAST(_dlt_load_id as STRING) > (select max(SAFE_CAST(_dlt_load_id as STRING)) from {{ this }})
    {% endif %}
)

select *
from raw