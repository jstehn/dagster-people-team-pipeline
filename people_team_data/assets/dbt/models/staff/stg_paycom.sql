{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select
        SAFE_CAST(employee_code as INT64) as employee_id, -- Potential PII
        SAFE_CAST(legal_firstname as STRING) as legal_firstname, -- PII
        SAFE_CAST(legal_lastname as STRING) as legal_lastname, -- PII
        SAFE.PARSE_DATE('%m/%d/%Y', hire_date) as hire_date,
        SAFE.PARSE_DATE('%m/%d/%Y', rehire_date) as rehire_date,
        SAFE.PARSE_DATE('%m/%d/%Y', termination_date) as termination_date,
        SAFE_CAST(employee_status as STRING) as employee_status,
        SAFE_CAST(work_email as STRING) as work_email, -- PII
        SAFE_CAST(gender as STRING) as gender, -- Potential PII
        SAFE_CAST(eeo1_ethnicity as STRING) as eeo1_ethnicity, -- Potential PII
        SAFE_CAST(salary as NUMERIC) as salary,
        SAFE_CAST(pay_type as STRING) as pay_type,
        SAFE_CAST(rate_1 as NUMERIC) as rate_1,
        SAFE_CAST(replace(annual_salary, ',', '') as FLOAT64) as annual_salary, -- Consider NUMERIC/DECIMAL if precision needed
        SAFE_CAST(department as STRING) as department,
        SAFE_CAST(sub_department_code as STRING) as sub_department_code,
        SAFE_CAST(department_desc as STRING) as department_desc,
        SAFE_CAST(sub_department_desc as STRING) as sub_department_desc,
        SAFE_CAST(sub_department_gl_code as STRING) as sub_department_gl_code,
        SAFE_CAST(department_gl_code as STRING) as department_gl_code,
        SAFE_CAST(division_code as STRING) as division_code,
        SAFE_CAST(division_desc as STRING) as division_desc,
        SAFE_CAST(division_gl_code as STRING) as division_gl_code,
        SAFE_CAST(work_location as STRING) as work_location,
        _dlt_load_id,
        _dlt_id,
        SAFE_CAST(FORMAT('%.0f', primary_phone) as STRING) as primary_phone, -- PII, Ensure FORMAT handles NULLs or use SAFE_CAST after format
        SAFE_CAST(seid AS STRING) as seid, -- PII, Changed cast to STRING based on BambooHR schema
        SAFE_CAST(ess_eeo1_ethnicity_race as STRING) as ess_eeo1_ethnicity_race, -- Potential PII
        SAFE_CAST(position as STRING) as position,
        SAFE_CAST(labor_allocation_profile as STRING) as labor_allocation_profile,
        SAFE_CAST(legal_middle_name as STRING) as legal_middle_name -- PII
    from {{ source('raw_staff_data', 'raw_paycom') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
