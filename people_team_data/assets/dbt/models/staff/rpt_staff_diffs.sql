{{ config(
    materialized='table'
) }}

with bamboohr as (
    select
        employee_id,
        first_name as bamboohr_first_name,
        last_name as bamboohr_last_name,
        middle_name as bamboohr_middle_name,
        gender as bamboohr_gender,
        email as bamboohr_email,
        employment_status as bamboohr_employment_status,
        hire_date as bamboohr_hire_date,
        division as bamboohr_division,
        job_title as bamboohr_position,
        location as bamboohr_location,
        pay_rate as bamboohr_pay_rate,
        pay_type as bamboohr_pay_type
    from {{ ref('stg_bamboohr') }}
),
paycom as (
    select
        employee_id,
        legal_firstname as paycom_first_name,
        legal_lastname as paycom_last_name,
        legal_middle_name as paycom_middle_name,
        gender as paycom_gender,
        work_email as paycom_email,
        employee_status as paycom_employment_status,
        hire_date as paycom_hire_date,
        division_code as paycom_division,
        position as paycom_position,
        work_location as paycom_location,
        rate_1 as paycom_hourly_rate,
        annual_salary as paycom_annual_salary,
        pay_type as paycom_pay_type
    from {{ ref('stg_paycom') }}
),
position_control as (
    select
        stg_position_control_employees.employee_id,
        employee_first_name as pc_first_name,
        employee_last_name as pc_last_name,
        employee_middle_name as pc_middle_name,
        employee_status as pc_employment_status,
        stg_position_control_positions.position_hr_division as pc_division,
        stg_position_control_positions.position_name as pc_position,
        assignment_salary as pc_salary,
        assignment_wage as pc_hourly_rate,
        case
        when assignment_wage is null
        then 'Salary'
        else 'Hourly' end as pc_pay_type
    from {{ ref('stg_position_control_employees') }}
    join {{ ref('stg_position_control_assignments') }}
    on stg_position_control_employees.employee_id = stg_position_control_assignments.employee_id
    join {{ ref('stg_position_control_positions') }}
    on stg_position_control_assignments.position_id = stg_position_control_positions.position_id
),
joined_data as (
    select
        coalesce(bamboohr.employee_id, paycom.employee_id, position_control.employee_id) as employee_id,
        coalesce(
            {% if target.name == 'duckdb_dev' %}
                TRY_CAST(bamboohr.bamboohr_first_name AS STRING),
                TRY_CAST(paycom.paycom_first_name AS STRING),
                TRY_CAST(position_control.pc_first_name AS STRING)
            {% else %}
                SAFE_CAST(bamboohr.bamboohr_first_name AS STRING),
                SAFE_CAST(paycom.paycom_first_name AS STRING),
                SAFE_CAST(position_control.pc_first_name AS STRING)
            {% endif %}
        ) as employee_first_name,
        coalesce(
            {% if target.name == 'duckdb_dev' %}
                TRY_CAST(bamboohr.bamboohr_last_name AS STRING),
                TRY_CAST(paycom.paycom_last_name AS STRING),
                TRY_CAST(position_control.pc_last_name AS STRING)
            {% else %}
                SAFE_CAST(bamboohr.bamboohr_last_name AS STRING),
                SAFE_CAST(paycom.paycom_last_name AS STRING),
                SAFE_CAST(position_control.pc_last_name AS STRING)
            {% endif %}
        ) as employee_last_name,
        {% if target.name == 'duckdb_dev' %}
            TRY_CAST(bamboohr.bamboohr_first_name AS STRING) as bhr_first_name,
            TRY_CAST(paycom.paycom_first_name AS STRING) as pc_first_name,
            TRY_CAST(position_control.pc_first_name AS STRING) as pce_first_name,
            TRY_CAST(bamboohr.bamboohr_last_name AS STRING) as bhr_last_name,
            TRY_CAST(paycom.paycom_last_name AS STRING) as pc_last_name,
            TRY_CAST(position_control.pc_last_name AS STRING) as pce_last_name,
            TRY_CAST(bamboohr.bamboohr_middle_name AS STRING) as bhr_middle_name,
            TRY_CAST(paycom.paycom_middle_name AS STRING) as pc_middle_name,
            TRY_CAST(position_control.pc_middle_name AS STRING) as pce_middle_name,
            TRY_CAST(bamboohr.bamboohr_hire_date AS STRING) as bhr_hire_date,
            TRY_CAST(paycom.paycom_hire_date AS STRING) as pc_hire_date,
            TRY_CAST(NULL AS STRING) as pce_hire_date,
            TRY_CAST(bamboohr.bamboohr_employment_status AS STRING) as bhr_status,
            TRY_CAST(paycom.paycom_employment_status AS STRING) as pc_status,
            TRY_CAST(position_control.pc_employment_status AS STRING) as pce_status,
            TRY_CAST(bamboohr.bamboohr_position AS STRING) as bhr_job_title,
            TRY_CAST(paycom.paycom_position AS STRING) as pc_job_title,
            TRY_CAST(position_control.pc_position AS STRING) as pce_job_title,
            TRY_CAST(bamboohr.bamboohr_division AS STRING) as bhr_division,
            TRY_CAST(paycom.paycom_division AS STRING) as pc_division,
            TRY_CAST(position_control.pc_division AS STRING) as pce_division,
            TRY_CAST(bamboohr.bamboohr_location AS STRING) as bhr_location,
            TRY_CAST(paycom.paycom_location AS STRING) as pc_location,
            TRY_CAST(NULL AS STRING) as pce_location,
            TRY_CAST(bamboohr.bamboohr_pay_type AS STRING) as bhr_pay_type,
            TRY_CAST(paycom.paycom_pay_type AS STRING) as pc_pay_type,
            TRY_CAST(position_control.pc_pay_type AS STRING) as pce_pay_type
        {% else %}
            SAFE_CAST(bamboohr.bamboohr_first_name AS STRING) as bhr_first_name,
            SAFE_CAST(paycom.paycom_first_name AS STRING) as pc_first_name,
            SAFE_CAST(position_control.pc_first_name AS STRING) as pce_first_name,
            SAFE_CAST(bamboohr.bamboohr_last_name AS STRING) as bhr_last_name,
            SAFE_CAST(paycom.paycom_last_name AS STRING) as pc_last_name,
            SAFE_CAST(position_control.pc_last_name AS STRING) as pce_last_name,
            SAFE_CAST(bamboohr.bamboohr_middle_name AS STRING) as bhr_middle_name,
            SAFE_CAST(paycom.paycom_middle_name AS STRING) as pc_middle_name,
            SAFE_CAST(position_control.pc_middle_name AS STRING) as pce_middle_name,
            SAFE_CAST(bamboohr.bamboohr_hire_date AS STRING) as bhr_hire_date,
            SAFE_CAST(paycom.paycom_hire_date AS STRING) as pc_hire_date,
            SAFE_CAST(NULL AS STRING) as pce_hire_date,
            SAFE_CAST(bamboohr.bamboohr_employment_status AS STRING) as bhr_status,
            SAFE_CAST(paycom.paycom_employment_status AS STRING) as pc_status,
            SAFE_CAST(position_control.pc_employment_status AS STRING) as pce_status,
            SAFE_CAST(bamboohr.bamboohr_position AS STRING) as bhr_job_title,
            SAFE_CAST(paycom.paycom_position AS STRING) as pc_job_title,
            SAFE_CAST(position_control.pc_position AS STRING) as pce_job_title,
            SAFE_CAST(bamboohr.bamboohr_division AS STRING) as bhr_division,
            SAFE_CAST(paycom.paycom_division AS STRING) as pc_division,
            SAFE_CAST(position_control.pc_division AS STRING) as pce_division,
            SAFE_CAST(bamboohr.bamboohr_location AS STRING) as bhr_location,
            SAFE_CAST(paycom.paycom_location AS STRING) as pc_location,
            SAFE_CAST(NULL AS STRING) as pce_location,
            SAFE_CAST(bamboohr.bamboohr_pay_type AS STRING) as bhr_pay_type,
            SAFE_CAST(paycom.paycom_pay_type AS STRING) as pc_pay_type,
            SAFE_CAST(position_control.pc_pay_type AS STRING) as pce_pay_type
        {% endif %}
    from bamboohr
    full outer join paycom on bamboohr.employee_id = paycom.employee_id
    full outer join position_control on coalesce(bamboohr.employee_id, paycom.employee_id) = position_control.employee_id
),
differences as (
    select
        employee_id,
        employee_first_name,
        employee_last_name,
        field,
        bamboohr_value,
        paycom_value,
        position_control_value
    from joined_data
    unpivot(
        (bamboohr_value, paycom_value, position_control_value)
        for field in (
            (bhr_first_name, pc_first_name, pce_first_name) as 'first_name',
            (bhr_last_name, pc_last_name, pce_last_name) as 'last_name',
            (bhr_middle_name, pc_middle_name, pce_middle_name) as 'middle_name',
            (bhr_hire_date, pc_hire_date, pce_hire_date) as 'hire_date',
            (bhr_status, pc_status, pce_status) as 'status',
            (bhr_job_title, pc_job_title, pce_job_title) as 'job_title',
            (bhr_division, pc_division, pce_division) as 'division',
            (bhr_location, pc_location, pce_location) as 'location',
            (bhr_pay_type, pc_pay_type, pce_pay_type) as 'pay_type'
        )
    )
    where
        -- Exclude rows where all values are NULL
        not (bamboohr_value is null and paycom_value is null and position_control_value is null)
        -- Check for differences, handling NULLs and making comparison case-insensitive
        and not (
            ((lower(coalesce(bamboohr_value, '##NULL##')) = lower(coalesce(position_control_value, '##NULL##')))
             or (bamboohr_value is null) or (position_control_value is null)) and
            ((lower(coalesce(paycom_value, '##NULL##')) = lower(coalesce(position_control_value, '##NULL##'))
             or (paycom_value is null) or (position_control_value is null))) and
            ((lower(coalesce(bamboohr_value, '##NULL##')) = lower(coalesce(paycom_value, '##NULL##'))
             or (bamboohr_value is null) or (paycom_value is null)))
        )
)

select
    employee_id,
    employee_first_name,
    employee_last_name,
    field,
    bamboohr_value,
    paycom_value,
    position_control_value
from differences
order by employee_id, field
