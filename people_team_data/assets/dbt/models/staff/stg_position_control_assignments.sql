{{ config(
    materialized='table',
    unique_key='assignment_id'
) }}


with raw as (
    select
        {% if target.name == 'duckdb_dev' %}
                TRY_CAST(assignment_id AS STRING) as assignment_id,
                TRY_CAST(assignment_count AS BIGINT) as assignment_count,
                TRY_CAST(employee_id AS BIGINT) as employee_id,
                TRY_CAST(employee_count AS BIGINT) as employee_count,
                TRY_CAST(assignment_full AS STRING) as assignment_full,
                TRY_CAST(assignment_scale AS STRING) as assignment_scale,
                TRY_CAST(assignment_start_number AS BIGINT) as assignment_start_number,
                TRY_CAST(assignment_end_number AS BIGINT) as assignment_end_number,
                TRY_CAST(_dlt_load_id AS STRING) as _dlt_load_id,
                TRY_CAST(_dlt_id AS STRING) as _dlt_id,
                CAST(try_strptime(CAST(assignment_start AS VARCHAR), '%Y-%m-%d') AS DATE) as assignment_start,
                CAST(try_strptime(CAST(assignment_end AS VARCHAR), '%Y-%m-%d') AS DATE) as assignment_end,
                TRY_CAST(assignment_status AS STRING) as assignment_status,
                TRY_CAST(position_id AS STRING) as position_id,
                TRY_CAST(position_full AS STRING) as position_full,
                TRY_CAST(employee_full AS STRING) as employee_full,
                TRY_CAST(assignment_fte AS DOUBLE) as assignment_fte,
                TRY_CAST(assignment_calendar AS DOUBLE) as assignment_calendar,
                TRY_CAST(assignment_step AS BIGINT) as assignment_step,
                TRY_CAST(assignment_salary AS DOUBLE) as assignment_salary,
                TRY_CAST(assignment_ppp AS DOUBLE) as assignment_ppp,
                TRY_CAST(position_account AS STRING) as position_account,
                TRY_CAST(position_goal AS STRING) as position_goal,
                TRY_CAST(position_hr_division AS STRING) as position_hr_division,
                TRY_CAST(notes AS STRING) as notes,
                TRY_CAST(assignment_wage AS DOUBLE) as assignment_wage,
                TRY_CAST(assignment_reporting3 AS STRING) as assignment_reporting3
        {% else %}
            SAFE_CAST(assignment_id as STRING) as assignment_id,
            SAFE_CAST(assignment_count as INT64) as assignment_count,
            SAFE_CAST(employee_id as INT64) as employee_id,
            SAFE_CAST(employee_count as INT64) as employee_count,
            SAFE_CAST(assignment_full as STRING) as assignment_full,
            SAFE_CAST(assignment_scale as STRING) as assignment_scale,
            SAFE_CAST(assignment_start_number as INT64) as assignment_start_number,
            SAFE_CAST(assignment_end_number as INT64) as assignment_end_number,
            SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
            SAFE_CAST(_dlt_id as STRING) as _dlt_id,
            SAFE.PARSE_DATE('%Y-%m-%d', assignment_start) as assignment_start,
            SAFE.PARSE_DATE('%Y-%m-%d', assignment_end) as assignment_end,
            SAFE_CAST(assignment_status as STRING) as assignment_status,
            SAFE_CAST(position_id as STRING) as position_id,
            SAFE_CAST(position_full as STRING) as position_full,
            SAFE_CAST(employee_full as STRING) as employee_full,
            SAFE_CAST(assignment_fte as DECIMAL) as assignment_fte,
            SAFE_CAST(assignment_calendar as DECIMAL) as assignment_calendar,
            SAFE_CAST(assignment_step as INT64) as assignment_step,
            SAFE_CAST(assignment_salary as DECIMAL) as assignment_salary,
            SAFE_CAST(assignment_ppp as DECIMAL) as assignment_ppp,
            SAFE_CAST(position_account as STRING) as position_account,
            SAFE_CAST(position_goal as STRING) as position_goal,
            SAFE_CAST(position_hr_division as STRING) as position_hr_division,
            SAFE_CAST(notes as STRING) as notes,
            SAFE_CAST(assignment_wage as DECIMAL) as assignment_wage,
            SAFE_CAST(assignment_reporting3 as STRING) as assignment_reporting3
        {% endif %}
    from {{ source('raw_staff_data', 'raw_position_control_assignments') }}
    {% if is_incremental() %}
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
