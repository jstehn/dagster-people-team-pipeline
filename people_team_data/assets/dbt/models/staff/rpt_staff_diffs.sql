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
        pay_rate as bamboohr_pay_rate, -- Changed from compensation_pay_rate
        pay_type as bamboohr_pay_type -- Changed from compensation_pay_type
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
        stg_position_control_employees.employee_id, -- Disambiguate employee_id
        employee_first_name as pc_first_name,
        employee_last_name as pc_last_name,
        employee_middle_name as pc_middle_name,
        employee_status as pc_employment_status,
        -- hire_date is not available in position control tables
        stg_position_control_positions.position_hr_division as pc_division,
        stg_position_control_positions.position_name as pc_position,
        -- work_location is not available in position control tables
        assignment_salary as pc_salary,
        assignment_wage as pc_hourly_rate,
        case
        when assignment_wage is null
        then "Salary"
        else "Hourly" end as pc_pay_type
    from {{ ref('stg_position_control_employees') }}
    join {{ ref('stg_position_control_assignments') }}
    on stg_position_control_employees.employee_id = stg_position_control_assignments.employee_id
    join {{ ref('stg_position_control_positions') }}
    on stg_position_control_assignments.position_id = stg_position_control_positions.position_id
),
joined_data as (
    select
        coalesce(bamboohr.employee_id, paycom.employee_id, position_control.employee_id) as employee_id,

        -- Coalesce names for final output
        coalesce(
            safe_cast(bamboohr.bamboohr_first_name as STRING),
            safe_cast(paycom.paycom_first_name as STRING),
            safe_cast(position_control.pc_first_name as STRING)
        ) as employee_first_name,
        coalesce(
            safe_cast(bamboohr.bamboohr_last_name as STRING),
            safe_cast(paycom.paycom_last_name as STRING),
            safe_cast(position_control.pc_last_name as STRING)
        ) as employee_last_name,

        -- Cast all fields to STRING for consistent comparison
        -- Personal Info
        safe_cast(bamboohr.bamboohr_first_name as STRING) as bhr_first_name,
        safe_cast(paycom.paycom_first_name as STRING) as pc_first_name,
        safe_cast(position_control.pc_first_name as STRING) as pce_first_name,

        safe_cast(bamboohr.bamboohr_last_name as STRING) as bhr_last_name,
        safe_cast(paycom.paycom_last_name as STRING) as pc_last_name,
        safe_cast(position_control.pc_last_name as STRING) as pce_last_name,

        safe_cast(bamboohr.bamboohr_middle_name as STRING) as bhr_middle_name,
        safe_cast(paycom.paycom_middle_name as STRING) as pc_middle_name,
        safe_cast(position_control.pc_middle_name as STRING) as pce_middle_name,

        -- Employment Info
        safe_cast(bamboohr.bamboohr_hire_date as STRING) as bhr_hire_date,
        safe_cast(paycom.paycom_hire_date as STRING) as pc_hire_date,
        safe_cast(NULL as STRING) as pce_hire_date, -- Explicit NULL for missing PCE hire_date

        safe_cast(bamboohr.bamboohr_employment_status as STRING) as bhr_status,
        safe_cast(paycom.paycom_employment_status as STRING) as pc_status,
        safe_cast(position_control.pc_employment_status as STRING) as pce_status,

        -- Job Info
        safe_cast(bamboohr.bamboohr_position as STRING) as bhr_job_title,
        safe_cast(paycom.paycom_position as STRING) as pc_job_title,
        safe_cast(position_control.pc_position as STRING) as pce_job_title, -- Use existing pc_position

        safe_cast(bamboohr.bamboohr_division as STRING) as bhr_division,
        safe_cast(paycom.paycom_division as STRING) as pc_division,
        safe_cast(position_control.pc_division as STRING) as pce_division, -- Use existing pc_division

        safe_cast(bamboohr.bamboohr_location as STRING) as bhr_location,
        safe_cast(paycom.paycom_location as STRING) as pc_location,
        safe_cast(NULL as STRING) as pce_location, -- Explicit NULL for missing PCE location

        -- Compensation Info
        safe_cast(bamboohr.bamboohr_pay_type as STRING) as bhr_pay_type,
        safe_cast(paycom.paycom_pay_type as STRING) as pc_pay_type,
        safe_cast(position_control.pc_pay_type as STRING) as pce_pay_type -- Use existing pc_pay_type

        -- Add other fields as needed, ensuring they exist in source CTEs and are cast to STRING

    from bamboohr
    full outer join paycom on bamboohr.employee_id = paycom.employee_id
    full outer join position_control on coalesce(bamboohr.employee_id, paycom.employee_id) = position_control.employee_id
),
differences as (
    select
        employee_id,
        employee_first_name, -- Include names
        employee_last_name,  -- Include names
        field,
        bamboohr_value,
        paycom_value,
        position_control_value
    from joined_data
    unpivot(
        (bamboohr_value, paycom_value, position_control_value) -- Target columns for values
        for field in ( -- Source columns representing fields to compare
            (bhr_first_name, pc_first_name, pce_first_name) as 'first_name',
            (bhr_last_name, pc_last_name, pce_last_name) as 'last_name',
            (bhr_middle_name, pc_middle_name, pce_middle_name) as 'middle_name',
            (bhr_hire_date, pc_hire_date, pce_hire_date) as 'hire_date', -- Use alias pce_hire_date
            (bhr_status, pc_status, pce_status) as 'status',
            (bhr_job_title, pc_job_title, pce_job_title) as 'job_title', -- Use alias pce_job_title
            (bhr_division, pc_division, pce_division) as 'division', -- Use alias pce_division
            (bhr_location, pc_location, pce_location) as 'location', -- Use alias pce_location
            (bhr_pay_type, pc_pay_type, pce_pay_type) as 'pay_type' -- Use alias pce_pay_type
            -- Add corresponding entries for other compared fields
        )
    )
    where
        -- Check for differences, handling NULLs and making comparison case-insensitive
        not (
            lower(coalesce(bamboohr_value, '##NULL##')) = lower(coalesce(paycom_value, '##NULL##'))
            and lower(coalesce(paycom_value, '##NULL##')) = lower(coalesce(position_control_value, '##NULL##'))
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