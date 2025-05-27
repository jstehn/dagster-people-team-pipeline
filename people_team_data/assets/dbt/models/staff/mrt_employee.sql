{{ config(
    materialized='table'
) }}

with employee_data as (
    select
        coalesce(bhr.employee_id, pc.employee_id, paycom.employee_id) as employee_id,
        coalesce(bhr.first_name, paycom.legal_firstname, pc.employee_first_name) as first_name,
        coalesce(bhr.last_name, paycom.legal_lastname, pc.employee_last_name) as last_name,
        coalesce(bhr.middle_name, paycom.legal_middle_name, pc.employee_middle_name) as middle_name,
        bhr.preferred_name as preferred_name,
        coalesce(paycom.work_email, bhr.email) as email_address,
        coalesce(paycom.gender, bhr.gender) as gender,
        bhr.school as school, -- Ensure `school` exists in `stg_bamboohr`
        coalesce(pc_assign.assignment_fte, bhr.fte) as fte,
        coalesce(paycom.work_location, bhr.location) as location, -- Fixed typo in `work_location`
        coalesce(paycom.seid, bhr.seid) as seid,
        coalesce(pc_assign.position_full, null) as position_id,
        coalesce(pc_assign.assignment_id, null) as assignment_id, -- Added missing comma
        coalesce(bhr.employment_status, paycom.employee_status, pc.employee_status) as employment_status,
        coalesce(paycom.hire_date, bhr.hire_date) as hire_date, -- Fixed `bhr_hire_date` to `bhr.hire_date`
        coalesce(paycom.termination_date, bhr.termination_date) as termination_date,
        coalesce(paycom.ess_eeo1_ethnicity_race, paycom.eeo1_ethnicity, bhr.ethnicity_and_race) as ethnicity_and_race,
        bhr.total_years_edu_service as total_years_edu_service,
        bhr.total_years_in_lea as total_years_in_lea,
        bhr.years_of_experience as years_of_experience_in_role,
        coalesce(paycom.pay_type, bhr.pay_type) as pay_type,
        coalesce(pc_assign.assignment_salary, paycom.annual_salary) as annual_salary,
        coalesce(pc_assign.assignment_wage, paycom.rate_1) as hourly_rate,
        bhr.date_of_birth as date_of_birth,
        paycom.department as department,
        paycom.sub_department_code as sub_department_code,
        paycom.department_desc as department_desc,
        paycom.sub_department_desc as sub_department_desc,
        coalesce(pc_pos.position_name, paycom.position, bhr.job_title) as position_name,
        bhr.reports_to as reports_to
    from {{ ref('stg_position_control_employees') }} pc
    left join {{ ref('stg_position_control_assignments') }} pc_assign
        on pc.employee_id = pc_assign.employee_id
    left join {{ ref('stg_paycom') }} paycom
        on pc.employee_id = paycom.employee_id
    left join {{ ref('stg_bamboohr') }} bhr
        on coalesce(pc.employee_id, paycom.employee_id) = bhr.employee_id
    left join {{ ref('stg_position_control_positions') }} pc_pos
        on pc_assign.position_id = pc_pos.position_id
)

select *
from employee_data
