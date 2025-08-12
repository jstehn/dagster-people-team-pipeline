{{ config(
    materialized='table',
    unique_key='stipend_id'
) }}


with raw as (
    select
        {% if target.name == 'duckdb_dev' %}
                TRY_CAST(stipend_id AS STRING) as stipend_id,
                TRY_CAST(employee_id AS BIGINT) as employee_id,
                TRY_CAST(employee_name AS STRING) as employee_name,
                CAST(try_strptime(CAST(stipend_start_date AS VARCHAR), '%Y-%m-%d') AS DATE) as stipend_start_date,
                CAST(try_strptime(CAST(stipend_end_date AS VARCHAR), '%Y-%m-%d') AS DATE) as stipend_end_date,
                TRY_CAST(stipend_status AS STRING) as stipend_status,
                TRY_CAST(stipend_category AS STRING) as stipend_category,
                TRY_CAST(stipend_description AS STRING) as stipend_description,
                TRY_CAST(stipend_amount AS DOUBLE) as stipend_amount,
                TRY_CAST(stipend_full_account AS STRING) as stipend_full_account,
                TRY_CAST(stipend_account AS STRING) as stipend_account,
                TRY_CAST(stipend_full_goal AS STRING) as stipend_full_goal,
                TRY_CAST(stipend_goal AS STRING) as stipend_goal,
                TRY_CAST(stipend_location AS STRING) as stipend_location,
                TRY_CAST(stipend_full_resource AS STRING) as stipend_full_resource,
                TRY_CAST(stipend_resource AS STRING) as stipend_resource,
                TRY_CAST(stipend_start_number AS BIGINT) as stipend_start_number,
                TRY_CAST(stipend_end_number AS BIGINT) as stipend_end_number,
                TRY_CAST(ytd_count AS BIGINT) as ytd_count,
                TRY_CAST(ytd_total AS DOUBLE) as ytd_total,
                TRY_CAST(future_count AS BIGINT) as future_count,
                TRY_CAST(future_total AS DOUBLE) as future_total,
                TRY_CAST(stipend_count AS BIGINT) as stipend_count,
                TRY_CAST(stipend_total AS DOUBLE) as stipend_total,
                TRY_CAST(_dlt_load_id AS STRING) as _dlt_load_id,
                TRY_CAST(_dlt_id AS STRING) as _dlt_id,
                TRY_CAST(employee_id__v_text AS STRING) as employee_id__v_text,
                TRY_CAST(notes AS STRING) as notes
        {% else %}
            SAFE_CAST(stipend_id as STRING) as stipend_id,
            SAFE_CAST(employee_id as INT64) as employee_id,
            SAFE_CAST(employee_name as STRING) as employee_name,
            SAFE.PARSE_DATE('%Y-%m-%d', stipend_start_date) as stipend_start_date,
            SAFE.PARSE_DATE('%Y-%m-%d', stipend_end_date) as stipend_end_date,
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
            SAFE_CAST(ytd_total as DECIMAL) as ytd_total,
            SAFE_CAST(future_count as INT64) as future_count,
            SAFE_CAST(future_total as DECIMAL) as future_total,
            SAFE_CAST(stipend_count as INT64) as stipend_count,
            SAFE_CAST(stipend_total as DECIMAL) as stipend_total,
            SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
            SAFE_CAST(_dlt_id as STRING) as _dlt_id,
            SAFE_CAST(employee_id__v_text as STRING) as employee_id__v_text,
            SAFE_CAST(notes as STRING) as notes
        {% endif %}
    from {{ source('raw_staff_data', 'raw_position_control_stipends') }}
    {% if is_incremental() %}
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
