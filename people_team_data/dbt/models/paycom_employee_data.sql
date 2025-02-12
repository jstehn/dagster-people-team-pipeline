{{ config(
    materialized='table'
) }}

SELECT
    TRY_TO_DATE(hire_date) AS hire_date,
    TRY_TO_DATE(rehire_date) AS rehire_date,
    TRY_TO_DATE(termination_date) AS termination_date,
    employee_status,
    legal_firstname,
    legal_lastname,
    work_email,
    gender,
    eeo1_ethnicity,
    TRY_TO_NUMBER(salary) AS salary,
    pay_type,
    TRY_TO_NUMBER(rate_1) AS rate_1,
    TRY_TO_NUMBER(annual_salary) AS annual_salary,
    employee_code,
    department,
    sub_department_code,
    department_desc,
    sub_department_desc,
    sub_department_gl_code,
    department_gl_code,
    division_code,
    division_desc,
    division_gl_code,
    work_location,
    paycom_data._dlt_load_id,
    paycom_data._dlt_id,
    primary_phone,
    seid,
    ess_eeo1_ethnicity_race,
    position,
    labor_allocation_profile,
    legal_middle_name,
    loads.schema_name,
    loads.inserted_at
FROM
    {{ source('paycom', 'paycom_data') }} AS paycom_data
LEFT JOIN
    {{ source('paycom', '_dlt_loads') }} AS loads
ON
    paycom_data._dlt_load_id = loads.load_id
