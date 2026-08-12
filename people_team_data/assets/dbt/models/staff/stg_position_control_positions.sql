{{ config(
    materialized='table',
    unique_key='position_id'
) }}


with raw as (
    select
        {% if target.name == 'duckdb_dev' %}
                TRY_CAST(position_id AS STRING) as position_id,
                TRY_CAST(position_unique AS BOOLEAN) as position_unique,
                TRY_CAST(assignment_count AS BIGINT) as assignment_count,
                CAST(try_strptime(CAST(position_start AS VARCHAR), '%Y-%m-%d') AS DATE) as position_start,
                TRY_CAST(position_status AS STRING) as position_status,
                TRY_CAST(position_hr_department AS STRING) as position_hr_department,
                TRY_CAST(position_hr_division AS STRING) as position_hr_division,
                TRY_CAST(position_hr_sub_department AS STRING) as position_hr_sub_department,
                TRY_CAST(position_code AS STRING) as position_code,
                TRY_CAST(position_count AS STRING) as position_count,
                TRY_CAST(position_name AS STRING) as position_name,
                TRY_CAST(position_account AS STRING) as position_account,
                TRY_CAST(position_goal AS STRING) as position_goal,
                TRY_CAST(position_full AS STRING) as position_full,
                TRY_CAST(position_credential AS STRING) as position_credential,
                TRY_CAST(friendly_names AS STRING) as friendly_names,
                TRY_CAST(salary_scale AS STRING) as salary_scale,
                TRY_CAST(position_school_level AS STRING) as position_school_level,
                TRY_CAST(position_start_number AS BIGINT) as position_start_number,
                TRY_CAST(position_end_number AS BIGINT) as position_end_number,
                TRY_CAST(_dlt_load_id AS STRING) as _dlt_load_id,
                TRY_CAST(_dlt_id AS STRING) as _dlt_id,
                -- Notes was removed from the source sheet; kept as a typed null
                -- placeholder so downstream consumers keep the column.
                TRY_CAST(null AS STRING) as notes
        {% else %}
            SAFE_CAST(position_id as STRING) as position_id,
            SAFE_CAST(position_unique as BOOLEAN) as position_unique,
            SAFE_CAST(assignment_count as INT64) as assignment_count,
            SAFE_CAST(position_start as DATE) as position_start,
            SAFE_CAST(position_status as STRING) as position_status,
            SAFE_CAST(position_hr_department as STRING) as position_hr_department,
            SAFE_CAST(position_hr_division as STRING) as position_hr_division,
            SAFE_CAST(position_hr_sub_department as STRING) as position_hr_sub_department,
            SAFE_CAST(position_code as STRING) as position_code,
            SAFE_CAST(position_count as STRING) as position_count,
            SAFE_CAST(position_name as STRING) as position_name,
            SAFE_CAST(position_account as STRING) as position_account,
            SAFE_CAST(position_goal as STRING) as position_goal,
            SAFE_CAST(position_full as STRING) as position_full,
            SAFE_CAST(position_credential as STRING) as position_credential,
            SAFE_CAST(friendly_names as STRING) as friendly_names,
            SAFE_CAST(salary_scale as STRING) as salary_scale,
            SAFE_CAST(position_school_level as STRING) as position_school_level,
            SAFE_CAST(position_start_number as INT64) as position_start_number,
            SAFE_CAST(position_end_number as INT64) as position_end_number,
            SAFE_CAST(_dlt_load_id as STRING) as _dlt_load_id,
            SAFE_CAST(_dlt_id as STRING) as _dlt_id,
            -- Notes was removed from the source sheet; kept as a typed null
            -- placeholder so downstream consumers keep the column.
            SAFE_CAST(null as STRING) as notes
        {% endif %}
    from {{ source('raw_staff_data', 'raw_position_control_positions') }}
    {% if is_incremental() %}
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
