{{ config(
    materialized='table'
) }}

with employee_data as (
    select
        coalesce(pc.employee_id, paycom.employee_id, bhr.employee_id) as employee_id,
        coalesce(paycom.legal_firstname, pc.employee_first_name, bhr.first_name) as first_name,
        coalesce(paycom.legal_lastname, pc.employee_last_name, bhr.last_name) as last_name,
        coalesce(paycom.work_email, pc.work_email, bhr.email) as email_address,
        coalesce(paycom.gender, pc.gender, bhr.gender) as gender,
        coalesce(bhr.school) as school,
        coalesce(pc.location, paycom.work_lcoation, bhr.location) as location,
        coalesce(pc.seid, bhr.seid) as seid,
        coalesce(pc_assign.position_full, null) as position_id,
        coalesce(pc_assign.assignment_id, null) as assignment_id
    from {{ ref('stg_position_control_employees') }} pc
    full outer join {{ ref('stg_position_control_assignments') }} pc_assign
        on pc.employee_id = pc_assign.employee_id
    full outer join {{ ref('stg_paycom') }} paycom
        on pc.employee_id = paycom.employee_id
    full outer join {{ ref('stg_bamboohr') }} bhr
        on coalesce(pc.employee_id, paycom.employee_id) = bhr.employee_id
)

select *
from employee_data