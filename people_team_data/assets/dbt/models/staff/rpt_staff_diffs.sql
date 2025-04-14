{{ config(
    materialized='table'
) }}

with bamboohr as (
    select 
        employee_id,
        first_name as bamboohr_first_name,
        last_name as bamboohr_last_name,
        gender as bamboohr_gender,
        email as bamboohr_email,
        employment_status as bamboohr_employment_status,
        hire_date as bamboohr_hire_date,
        job_information_division as bamboohr_division,
        job_information_job_title as bamboohr_position,
        compensation_pay_rate as bamboohr_salary,
        compensation_pay_type as bamboohr_pay_type
    from {{ ref('stg_bamboohr') }}
),
paycom as (
    select 
        employee_id,
        legal_firstname as paycom_first_name,
        legal_lastname as paycom_last_name,
        gender as paycom_gender,
        work_email as paycom_email,
        employee_status as paycom_employment_status,
        hire_date as paycom_hire_date,
        division_code as paycom_division,
        position as paycom_position,
        salary as paycom_salary,
        pay_type as paycom_pay_type
    from {{ ref('stg_paycom') }}
),
position_control as (
    select 
        employee_id,
        employee_first_name as pc_first_name,
        employee_last_name as pc_last_name,
        employee_status as pc_employment_status,
        hire_date as pc_hire_date,
        position_hr_division as pc_division,
        position_name as pc_position,
        assignment_salary as pc_salary
    from {{ ref('stg_position_control_employees') }}
    join {{ ref('stg_position_control_assignments') }}
    on stg_position_control_employees.employee_id = stg_position_control_assignments.employee_id
    join {{ ref('stg_position_control_positions') }}
    on stg_position_control_assignments.position_id = stg_position_control_positions.position_id
),
joined_data as (
    select 
        coalesce(bamboohr.employee_id, paycom.employee_id, position_control.employee_id) as employee_id,
        bamboohr.bamboohr_first_name,
        paycom.paycom_first_name,
        position_control.pc_first_name,
        bamboohr.bamboohr_last_name,
        paycom.paycom_last_name,
        position_control.pc_last_name,
        bamboohr.bamboohr_gender,
        paycom.paycom_gender,
        bamboohr.bamboohr_email,
        paycom.paycom_email,
        bamboohr.bamboohr_employment_status,
        paycom.paycom_employment_status,
        position_control.pc_employment_status,
        bamboohr.bamboohr_hire_date,
        paycom.paycom_hire_date,
        position_control.pc_hire_date,
        bamboohr.bamboohr_division,
        paycom.paycom_division,
        position_control.pc_division,
        bamboohr.bamboohr_position,
        paycom.paycom_position,
        position_control.pc_position,
        bamboohr.bamboohr_salary,
        paycom.paycom_salary,
        position_control.pc_salary,
        bamboohr.bamboohr_pay_type,
        paycom.paycom_pay_type
    from bamboohr
    full outer join paycom on bamboohr.employee_id = paycom.employee_id
    full outer join position_control on bamboohr.employee_id = position_control.employee_id
),
differences as (
    select
        employee_id,
        'first_name' as field,
        bamboohr_first_name as bamboohr_value,
        paycom_first_name as paycom_value,
        pc_first_name as position_control_value
    from joined_data
    where lower(bamboohr_first_name) <> lower(paycom_first_name)
       or lower(bamboohr_first_name) <> lower(pc_first_name)
       or lower(paycom_first_name) <> lower(pc_first_name)

    union all

    select
        employee_id,
        'last_name' as field,
        bamboohr_last_name as bamboohr_value,
        paycom_last_name as paycom_value,
        pc_last_name as position_control_value
    from joined_data
    where lower(bamboohr_last_name) <> lower(paycom_last_name)
       or lower(bamboohr_last_name) <> lower(pc_last_name)
       or lower(paycom_last_name) <> lower(pc_last_name)

    union all

    select
        employee_id,
        'gender' as field,
        bamboohr_gender as bamboohr_value,
        paycom_gender as paycom_value,
        null as position_control_value
    from joined_data
    where lower(bamboohr_gender) <> lower(paycom_gender)

    union all

    select
        employee_id,
        'email' as field,
        bamboohr_email as bamboohr_value,
        paycom_email as paycom_value,
        null as position_control_value
    from joined_data
    where lower(bamboohr_email) <> lower(paycom_email)

    union all

    select
        employee_id,
        'employment_status' as field,
        bamboohr_employment_status as bamboohr_value,
        paycom_employment_status as paycom_value,
        pc_employment_status as position_control_value
    from joined_data
    where lower(bamboohr_employment_status) <> lower(paycom_employment_status)
       or lower(bamboohr_employment_status) <> lower(pc_employment_status)
       or lower(paycom_employment_status) <> lower(pc_employment_status)

    union all

    select
        employee_id,
        'hire_date' as field,
        bamboohr_hire_date as bamboohr_value,
        paycom_hire_date as paycom_value,
        pc_hire_date as position_control_value
    from joined_data
    where bamboohr_hire_date <> paycom_hire_date
       or bamboohr_hire_date <> pc_hire_date
       or paycom_hire_date <> pc_hire_date

    union all

    select
        employee_id,
        'division' as field,
        bamboohr_division as bamboohr_value,
        paycom_division as paycom_value,
        pc_division as position_control_value
    from joined_data
    where lower(bamboohr_division) <> lower(paycom_division)
       or lower(bamboohr_division) <> lower(pc_division)
       or lower(paycom_division) <> lower(pc_division)

    union all

    select
        employee_id,
        'position' as field,
        bamboohr_position as bamboohr_value,
        paycom_position as paycom_value,
        pc_position as position_control_value
    from joined_data
    where lower(bamboohr_position) <> lower(paycom_position)
       or lower(bamboohr_position) <> lower(pc_position)
       or lower(paycom_position) <> lower(pc_position)

    union all

    select
        employee_id,
        'salary' as field,
        cast(bamboohr_salary as string) as bamboohr_value,
        cast(paycom_salary as string) as paycom_value,
        cast(pc_salary as string) as position_control_value
    from joined_data
    where bamboohr_salary <> paycom_salary
       or bamboohr_salary <> pc_salary
       or paycom_salary <> pc_salary

    union all

    select
        employee_id,
        'pay_type' as field,
        bamboohr_pay_type as bamboohr_value,
        paycom_pay_type as paycom_value,
        null as position_control_value
    from joined_data
    where lower(bamboohr_pay_type) <> lower(paycom_pay_type)
)
select *
from differences