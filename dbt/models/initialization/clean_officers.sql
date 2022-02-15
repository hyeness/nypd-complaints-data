{{ config(materialized='table' ) }}
SELECT 
    taxid, 
    full_name,
    first_name,
    last_name,
    command,
    rank,
    shield_no,
    appt_date,
    assignment_date,
    ethnicity

FROM {{ source('nypd_raw_data', 'officers') }}