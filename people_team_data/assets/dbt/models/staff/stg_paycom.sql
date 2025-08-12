{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select
        {% if target.name == 'duckdb_dev' %}
            TRY_CAST(employee_code AS BIGINT) as employee_id, -- Potential PII
            TRY_CAST(legal_firstname AS STRING) as legal_firstname, -- PII
            TRY_CAST(legal_lastname AS STRING) as legal_lastname, -- PII
            CAST(try_strptime(hire_date, '%m/%d/%Y') AS DATE) as hire_date,
            CAST(try_strptime(rehire_date, '%m/%d/%Y') AS DATE) as rehire_date,
            CAST(try_strptime(termination_date, '%m/%d/%Y') AS DATE) as termination_date,
            TRY_CAST(employee_status AS STRING) as employee_status,
            TRY_CAST(work_email AS STRING) as work_email, -- PII
            TRY_CAST(gender AS STRING) as gender, -- Potential PII
            TRY_CAST(eeo1_ethnicity AS STRING) as eeo1_ethnicity, -- Potential PII
            TRY_CAST(salary AS DOUBLE) as salary,
            TRY_CAST(pay_type AS STRING) as pay_type,
            TRY_CAST(rate_1 AS DOUBLE) as rate_1,
            TRY_CAST(REPLACE(annual_salary, ',', '') AS DOUBLE) as annual_salary, -- Consider NUMERIC/DECIMAL if precision needed
            TRY_CAST(department AS STRING) as department,
            TRY_CAST(sub_department_code AS STRING) as sub_department_code,
            TRY_CAST(department_desc AS STRING) as department_desc,
            TRY_CAST(sub_department_desc AS STRING) as sub_department_desc,
            TRY_CAST(sub_department_gl_code AS STRING) as sub_department_gl_code,
            TRY_CAST(department_gl_code AS STRING) as department_gl_code,
            TRY_CAST(division_code AS STRING) as division_code,
            TRY_CAST(division_desc AS STRING) as division_desc,
            TRY_CAST(division_gl_code AS STRING) as division_gl_code,
            TRY_CAST(work_location AS STRING) as work_location,
            _dlt_load_id,
            _dlt_id,
            TRY_CAST(CAST(primary_phone AS BIGINT) AS STRING) as primary_phone, -- PII
            TRY_CAST(seid AS STRING) as seid, -- PII
            TRY_CAST(ess_eeo1_ethnicity_race AS STRING) as ess_eeo1_ethnicity_race, -- Potential PII
            TRY_CAST(position AS STRING) as position,
            TRY_CAST(labor_allocation_profile AS STRING) as labor_allocation_profile,
            TRY_CAST(legal_middle_name AS STRING) as legal_middle_name -- PII
        {% elif target.name == 'staff' %}
            SAFE_CAST(employee_code AS INT64) as employee_id, -- Potential PII
            SAFE_CAST(legal_firstname AS STRING) as legal_firstname, -- PII
            SAFE_CAST(legal_lastname AS STRING) as legal_lastname, -- PII
            SAFE.PARSE_DATE('%m/%d/%Y', hire_date) as hire_date,
            SAFE.PARSE_DATE('%m/%d/%Y', rehire_date) as rehire_date,
            SAFE.PARSE_DATE('%m/%d/%Y', termination_date) as termination_date,
            SAFE_CAST(employee_status AS STRING) as employee_status,
            SAFE_CAST(work_email AS STRING) as work_email, -- PII
            SAFE_CAST(gender AS STRING) as gender, -- Potential PII
            SAFE_CAST(eeo1_ethnicity AS STRING) as eeo1_ethnicity, -- Potential PII
            SAFE_CAST(salary AS NUMERIC) as salary,
            SAFE_CAST(pay_type AS STRING) as pay_type,
            SAFE_CAST(rate_1 AS NUMERIC) as rate_1,
            SAFE_CAST(REPLACE(annual_salary, ',', '') AS FLOAT64) as annual_salary, -- Consider NUMERIC/DECIMAL if precision needed
            SAFE_CAST(department AS STRING) as department,
            SAFE_CAST(sub_department_code AS STRING) as sub_department_code,
            SAFE_CAST(department_desc AS STRING) as department_desc,
            SAFE_CAST(sub_department_desc AS STRING) as sub_department_desc,
            SAFE_CAST(sub_department_gl_code AS STRING) as sub_department_gl_code,
            SAFE_CAST(department_gl_code AS STRING) as department_gl_code,
            SAFE_CAST(division_code AS STRING) as division_code,
            SAFE_CAST(division_desc AS STRING) as division_desc,
            SAFE_CAST(division_gl_code AS STRING) as division_gl_code,
            SAFE_CAST(work_location AS STRING) as work_location,
            _dlt_load_id,
            _dlt_id,
            SAFE_CAST(FORMAT('%d', primary_phone) AS STRING) as primary_phone, -- PII
            SAFE_CAST(seid AS STRING) as seid, -- PII
            SAFE_CAST(ess_eeo1_ethnicity_race AS STRING) as ess_eeo1_ethnicity_race, -- Potential PII
            SAFE_CAST(position AS STRING) as position,
            SAFE_CAST(labor_allocation_profile AS STRING) as labor_allocation_profile,
            SAFE_CAST(legal_middle_name AS STRING) as legal_middle_name -- PII
        {% endif %}
    from {{ source('raw_staff_data', 'raw_paycom') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
