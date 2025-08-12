{{ config(
    materialized='table',
    unique_key='employee_id'
) }}


with raw as (
    select
        {% if target.name == 'duckdb_dev' %}
            TRY_CAST(employee_id AS BIGINT) as employee_id,
            TRY_CAST(employee_first_name AS STRING) as employee_first_name,
            TRY_CAST(employee_last_name AS STRING) as employee_last_name,
            TRY_CAST(employee_full_name AS STRING) as employee_full_name,
            TRY_CAST(employee_full AS STRING) as employee_full,
            TRY_CAST(employee_middle_name AS STRING) as employee_middle_name,
            TRY_CAST(employee_status AS STRING) as employee_status,
            TRY_CAST(_dlt_load_id AS STRING) as _dlt_load_id,
            TRY_CAST(_dlt_id AS STRING) as _dlt_id
        {% else %}
            SAFE_CAST(employee_id as INT64) as employee_id,
            SAFE_CAST(employee_first_name as STRING) as employee_first_name,
            SAFE_CAST(employee_last_name as STRING) as employee_last_name,
            SAFE_CAST(employee_full_name as STRING) as employee_full_name,
            SAFE_CAST(employee_full as STRING) as employee_full,
            SAFE_CAST(employee_middle_name as STRING) as employee_middle_name,
            SAFE_CAST(employee_status as STRING) as employee_status,
            SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
            SAFE_CAST(_dlt_id as STRING) as _dlt_id
        {% endif %}
    from {{ source('raw_staff_data', 'raw_position_control_employees') }}
    {% if is_incremental() %}
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
