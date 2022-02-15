{{ config(materialized='table' ) }}

WITH clean_lawsuits as (
    SELECT 
        TRIM(both '()' from tax_id)::float taxid,
        lit_start,
        disp_date,
        docket_no,
        ROW_NUMBER() OVER(
            PARTITION BY TRIM(both '()' from tax_id)::float, lit_start ORDER BY COALESCE(disp_date, '1999-01-01') DESC) 
            as row_num,
        CASE WHEN use_of_force = 'Y' THEN 1 ELSE 0 END use_of_force,
        CASE WHEN assault_battery = 'Y' THEN 1 ELSE 0 END assault_battery,
        CASE WHEN malicious_prosecution = 'Y' THEN 1 ELSE 0 END malicious_prosecution,
        CASE WHEN false_arrest = 'Y' THEN 1 ELSE 0 END false_arrest,
        court,
        disposition,
        payout_amt,
        split_part(defendants, '-', 1) defendant_name
    FROM {{ source('nypd_raw_data', 'lawsuits') }}
    WHERE tax_id IS NOT NULL 
)

SELECT
    taxid, 
    defendant_name,
    lit_start, 
    disp_date, 
    docket_no, 
    use_of_force, 
    assault_battery, 
    malicious_prosecution, 
    false_arrest, 
    court,
    disposition,
    payout_amt
FROM clean_lawsuits
WHERE row_num=1
