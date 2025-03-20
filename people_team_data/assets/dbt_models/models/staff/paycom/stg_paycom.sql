{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select 
        SAFE.PARSE_DATE('%m/%d/%Y', hire_date) as hire_date,
        SAFE.PARSE_DATE('%m/%d/%Y', rehire_date) as rehire_date,
        SAFE.PARSE_DATE('%m/%d/%Y', termination_date) as termination_date,
        trim(employee_status) as employee_status,
        trim(legal_firstname) as legal_firstname,
        trim(legal_lastname) as legal_lastname,
        trim(work_email) as work_email,
        trim(gender) as gender,
        trim(eeo1_ethnicity) as eeo1_ethnicity,
        cast(salary as numeric) as salary,
        trim(pay_type) as pay_type,
        cast(rate_1 as numeric) as rate_1,
        cast(replace(trim(annual_salary), ',', '') as float64) as annual_salary,
        cast(employee_code as int) as employee_id,
        trim(department) as department,
        trim(sub_department_code) as sub_department_code,
        trim(department_desc) as department_desc,
        trim(sub_department_desc) as sub_department_desc,
        trim(sub_department_gl_code) as sub_department_gl_code,
        trim(department_gl_code) as department_gl_code,
        trim(division_code) as division_code,
        trim(division_desc) as division_desc,
        trim(division_gl_code) as division_gl_code,
        trim(work_location) as work_location,
        _dlt_load_id,
        _dlt_id,
        FORMAT('%.0f', primary_phone) as primary_phone,
        CAST(seid AS INT) as seid,
        trim(ess_eeo1_ethnicity_race) as ess_eeo1_ethnicity_race,
        trim(position) as position,
        trim(labor_allocation_profile) as labor_allocation_profile,
        trim(legal_middle_name) as legal_middle_name
    from {{ source('raw_staff_data', 'raw_paycom') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw