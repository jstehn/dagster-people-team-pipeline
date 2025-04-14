{{ config(
    materialized='table'
) }}

with bamboohr as (
    select * from {{ ref('stg_bamboohr') }}
),
paycom as (
    select * from {{ ref('stg_paycom') }}
),
position_control as (
    select * from {{ ref('stg_position_control_employees') }}
    join {{ ref('stg_position_control_assignments') }}
    on stg_position_control_employees.id = stg_position_control_assignments.employee_id
    join {{ ref('stg_position_control_positions') }}
    on stg_position_control_assignments.position_id = stg_position_control_positions.id
)
select
    *
from bamboohr