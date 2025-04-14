{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select
        SAFE_CAST(employee_number as INT64) as employee_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(middle_name) as middle_name,
        trim(preferred_name) as preferred_name,
        trim(gender) as gender,
        trim(gender_identity) as gender_identity,
        trim(employee_pronouns) as employee_pronouns,
        SAFE_CAST(trim(age) as INT64) as age,
        SAFE.PARSE_DATE('%Y-%m-%d', date_of_birth) as date_of_birth,
        trim(email) as email,
        SAFE_CAST(regexp_replace(work_phone, r'[^0-9]', '') as INT64) as work_phone,
        trim(employment_status) as employment_status,
        SAFE.PARSE_DATE('%Y-%m-%d', hire_date) as hire_date,
        SAFE.PARSE_DATE('%Y-%m-%d', original_hire_date) as termination_date,
        trim(job_information_division) as job_information_division,
        trim(job_information_job_title) as job_information_job_title,
        trim(job_information_location) as job_information_location,
        trim(job_information_reports_to) as job_information_reports_to,
        SAFE.PARSE_DATE('%Y-%m-%d', compensation_effective_date) as compensation_effective_date,
        cast(regexp_replace(compensation_pay_rate, r'[^0-9.]', '') as decimal) as compensation_pay_rate,
        trim(compensation_pay_type) as compensation_pay_type,
        trim(compensation_pay_schedule) as compensation_pay_schedule,
        trim(compensation_overtime_status) as compensation_overtime_status,
        trim(compensation_change_reason) as compensation_change_reason,
        SAFE_CAST(custom_field4554 as INT64) as years_of_experience_in_role,
        SAFE_CAST(trim(custom_field4473) as INT64) as total_years_in_lea,
        SAFE_CAST(trim(custom_field4472) as INT64) as total_years_edu_service,
        cast(last_changed as timestamp) as last_changed,
        (custom_field4475 = 'Yes') as is_hispanic,
        trim(custom_field4476) as ethnicity_and_race,
        trim(custom_field4477) as school,
        cast(custom_field4505 as decimal) as fte,
        trim(custom_field4506) as highest_level_education,
        trim(custom_table4556) as credential_document_type,
        trim(custom_table4560) as credential_subject,
        cast(trim(custom_table4556) as string) as credential_document_number,
        custom_table4559 as credential_term,
        custom_table4590 as credential_status,
        trim(custom_table4558) as credential_additional_authorization,
        trim(custom_table4487) as credential_title,
        SAFE.PARSE_DATE('%Y-%m-%d', custom_table4463) as credential_effective_date,
        SAFE.PARSE_DATE('%Y-%m-%d', custom_table4464) as credential_expiration_date,
        SAFE.PARSE_DATE('%Y-%m-%d', custom_table4465) as credential_renewal_date,
        trim(custom_field4507) as job_classification_code,
        cast(last_changed_iso as timestamp) as last_changed_iso,
        created_by_user_id as created_by_user_id,
        trim(job_information_department) as job_information_department,
        SAFE_CAST(trim(custom_field4460) as INT64) as seid
    from {{ source('raw_staff_data', 'raw_bamboohr') }}
    {% if is_incremental() %}
      -- Only process new rows: adjust the filter as needed (using _dlt_load_id or an updated timestamp)
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select * 
from raw