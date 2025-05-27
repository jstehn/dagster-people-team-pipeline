{{ config(
    materialized='table',
    unique_key='employee_id'
) }}

with raw as (

    select
        -- Personal Information
        SAFE_CAST(employee_number as INT64) as employee_id,
        SAFE_CAST(first_name as STRING) as first_name,
        SAFE_CAST(last_name as STRING) as last_name,
        SAFE_CAST(middle_name as STRING) as middle_name,
        SAFE_CAST(preferred_name as STRING) as preferred_name,
        SAFE_CAST(gender as STRING) as gender,
        SAFE_CAST(gender_identity as STRING) as gender_identity,
        SAFE_CAST(employee_pronouns as STRING) as employee_pronouns,
        SAFE_CAST(age as INT64) as age, -- Assuming age comes as number/text convertible to INT64
        SAFE.PARSE_DATE('%Y-%m-%d', date_of_birth) as date_of_birth,
        SAFE_CAST(marital_status as STRING) as marital_status,
        -- national_id is PII, consider tokenization/masking if needed downstream
        SAFE_CAST(national_id as STRING) as national_id,
        SAFE_CAST(nationality as STRING) as nationality,
        SAFE_CAST(citizenship as STRING) as citizenship,

        -- Contact Information
        SAFE_CAST(email as STRING) as email,
        SAFE_CAST(home_email as STRING) as home_email,
        SAFE_CAST(work_phone as STRING) as work_phone, -- Keep as STRING
        SAFE_CAST(work_phone_ext as STRING) as work_phone_ext,
        SAFE_CAST(mobile_phone as STRING) as mobile_phone,
        SAFE_CAST(home_phone as STRING) as home_phone,

        -- Address Information
        SAFE_CAST(address_line_one as STRING) as address_line_one,
        SAFE_CAST(address_line_two as STRING) as address_line_two,
        SAFE_CAST(city as STRING) as city,
        SAFE_CAST(state as STRING) as state,
        SAFE_CAST(zipcode as STRING) as zipcode,
        SAFE_CAST(country as STRING) as country,

        -- Employment Information
        SAFE_CAST(employment_status as STRING) as employment_status,
        SAFE.PARSE_DATE('%Y-%m-%d', hire_date) as hire_date,
        SAFE.PARSE_DATE('%Y-%m-%d', original_hire_date) as original_hire_date,
        SAFE.PARSE_DATE('%Y-%m-%d', termination_date) as termination_date,

        -- Job Information
        SAFE_CAST(job_information_department as STRING) as department,
        SAFE_CAST(job_information_division as STRING) as division,
        SAFE_CAST(job_information_job_title as STRING) as job_title,
        SAFE_CAST(job_information_location as STRING) as location,
        SAFE_CAST(job_information_reports_to as STRING) as reports_to,
        SAFE_CAST(pay_group as STRING) as pay_group,
        SAFE_CAST(flsa_code as STRING) as flsa_code,
        SAFE_CAST(eeo_job_category as STRING) as eeo_job_category,

        -- Compensation
        SAFE_CAST(compensation_change_reason as STRING) as compensation_change_reason,
        SAFE.PARSE_DATE('%Y-%m-%d', compensation_effective_date) as compensation_effective_date,
        SAFE_CAST(regexp_replace(compensation_pay_rate, r'[^0-9.]', '') as DECIMAL) as pay_rate,
        SAFE_CAST(compensation_pay_type as STRING) as pay_type,
        SAFE_CAST(compensation_pay_schedule as STRING) as pay_schedule,
        SAFE_CAST(compensation_overtime_status as STRING) as overtime_status,

        -- Custom CALPADS Fields
        SAFE_CAST(custom_field4460 as STRING) as seid,
        SAFE_CAST(custom_field4472 as INT64) as total_years_edu_service,
        SAFE_CAST(custom_field4473 as INT64) as total_years_in_lea,
        SAFE_CAST(custom_field4554 as INT64) as years_of_experience,
        (custom_field4475 = 'Yes') as is_hispanic, -- Corrected logic based on schema description
        SAFE_CAST(custom_field4476 as STRING) as ethnicity_and_race,
        SAFE_CAST(custom_field4477 as STRING) as school,
        SAFE_CAST(custom_field4505 as DECIMAL) as fte,
        SAFE_CAST(custom_field4506 as STRING) as highest_education,
        SAFE_CAST(custom_field4507 as STRING) as job_classification_code,
        SAFE_CAST(custom_field4508 as STRING) as status_of_employment,
        SAFE_CAST(custom_field4597 as STRING) as prior_last_name,

        -- Education
        SAFE_CAST(education_college_or_institution as STRING) as college,
        SAFE_CAST(education_degree as STRING) as degree,
        SAFE.PARSE_DATE('%Y-%m-%d', education_start_date) as education_start_date,
        SAFE.PARSE_DATE('%Y-%m-%d', education_end_date) as education_end_date,
        SAFE_CAST(education_major_or_specialization as STRING) as major,
        SAFE_CAST(education_gpa as DECIMAL) as gpa,

        -- Credentials and Certifications (Custom Table_11)
        SAFE.PARSE_DATE('%Y-%m-%d', custom_table4463) as credential_effective_date,
        SAFE.PARSE_DATE('%Y-%m-%d', custom_table4464) as credential_expiration_date,
        SAFE.PARSE_DATE('%Y-%m-%d', custom_table4465) as credential_renewal_date,
        SAFE_CAST(custom_table4466 as STRING) as credential_comments,
        SAFE_CAST(custom_table4487 as STRING) as credential_title,
        SAFE_CAST(custom_table4556 as STRING) as credential_document_type,
        SAFE_CAST(custom_table4557 as STRING) as credential_document_number,
        SAFE_CAST(custom_table4558 as STRING) as credential_additional_auth,
        SAFE_CAST(custom_table4559 as STRING) as credential_term,
        SAFE_CAST(custom_table4560 as STRING) as credential_subject,
        SAFE_CAST(custom_table4590 as STRING) as credential_status,

        -- System Fields
        last_changed as last_changed, -- Assuming ISO 8601 format
        last_changed_iso as last_changed_iso, -- Assuming ISO 8601 format
        SAFE_CAST(created_by_user_id as STRING) as created_by_user_id,

        -- Add dlt metadata if needed for incremental logic
        _dlt_load_id,
        _dlt_id

    from {{ source('raw_staff_data', 'raw_bamboohr') }}
    {% if is_incremental() %}
      -- Ensure filtering column exists in the source and is appropriate
      where _dlt_load_id > (select max(_dlt_load_id) from {{ this }})
    {% endif %}
)

select *
from raw
