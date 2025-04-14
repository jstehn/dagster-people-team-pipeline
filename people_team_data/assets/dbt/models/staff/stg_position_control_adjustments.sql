{{ config(
    materialized='table',
    unique_key='adjustment_id'
) }}

with raw as (

    select 
        adjustment_begin_payroll as adjustment_begin_payroll,
        adjustment_end_payroll as adjustment_end_payroll,
        trim(adjustment_id) as adjustment_id,
        trim(assignment_full) as assignment_full,
        trim(adjustment_status) as adjustment_status,
        trim(adjustment_category) as adjustment_category,
        trim(adjustment_description) as adjustment_description,
        cast(adjustment_salary as decimal) as adjustment_salary,
        cast(adjustmentxx as int) as adjustmentxx,
        cast(adjustment_ppp as decimal) as adjustment_ppp,
        trim(assignment_location) as assignment_location,
        cast(adjustment_count as int) as adjustment_count,
        cast(adjustment_total as decimal) as adjustment_total,
        trim(notes) as notes,
        trim(_dlt_load_id) as _dlt_load_id,
        trim(_dlt_id) as _dlt_id,
    from {{ source('raw_staff_data', 'raw_position_control_adjustments') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw