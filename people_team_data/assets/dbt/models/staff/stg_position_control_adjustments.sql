{{ config(
    materialized='table',
    unique_key='adjustment_id'
) }}


with raw as (
    select
        {% if target.name == 'duckdb_dev' %}
            TRY_CAST(adjustment_begin_payroll AS STRING) as adjustment_begin_payroll,
            TRY_CAST(adjustment_end_payroll AS STRING) as adjustment_end_payroll,
            TRY_CAST(adjustment_id AS STRING) as adjustment_id,
            TRY_CAST(assignment_full AS STRING) as assignment_full,
            TRY_CAST(adjustment_status AS STRING) as adjustment_status,
            TRY_CAST(adjustment_category AS STRING) as adjustment_category,
            TRY_CAST(adjustment_description AS STRING) as adjustment_description,
            TRY_CAST(adjustment_salary AS DOUBLE) as adjustment_salary,
            TRY_CAST(adjustmentxx AS BIGINT) as adjustmentxx,
            TRY_CAST(adjustment_ppp AS DOUBLE) as adjustment_ppp,
            TRY_CAST(assignment_location AS STRING) as assignment_location,
            TRY_CAST(adjustment_count AS BIGINT) as adjustment_count,
            TRY_CAST(adjustment_total AS DOUBLE) as adjustment_total,
            TRY_CAST(notes AS STRING) as notes,
            TRY_CAST(_dlt_load_id AS STRING) as _dlt_load_id,
            TRY_CAST(_dlt_id AS STRING) as _dlt_id
        {% else %}
            SAFE_CAST(adjustment_begin_payroll as STRING) as adjustment_begin_payroll,
            SAFE_CAST(adjustment_end_payroll as STRING) as adjustment_end_payroll,
            SAFE_CAST(adjustment_id as STRING) as adjustment_id,
            SAFE_CAST(assignment_full as STRING) as assignment_full,
            SAFE_CAST(adjustment_status as STRING) as adjustment_status,
            SAFE_CAST(adjustment_category as STRING) as adjustment_category,
            SAFE_CAST(adjustment_description as STRING) as adjustment_description,
            SAFE_CAST(adjustment_salary as DECIMAL) as adjustment_salary,
            SAFE_CAST(adjustmentxx as INT64) as adjustmentxx,
            SAFE_CAST(adjustment_ppp as DECIMAL) as adjustment_ppp,
            SAFE_CAST(assignment_location as STRING) as assignment_location,
            SAFE_CAST(adjustment_count as INT64) as adjustment_count,
            SAFE_CAST(adjustment_total as DECIMAL) as adjustment_total,
            SAFE_CAST(notes as STRING) as notes,
            SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
            SAFE_CAST(_dlt_id as STRING) as _dlt_id
        {% endif %}
    from {{ source('raw_staff_data', 'raw_position_control_adjustments') }}
    {% if is_incremental() %}
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
