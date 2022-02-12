SELECT 
    taxid, 
    COALESCE(arrest_count, 0) arrest_count, 
    COALESCE(arrests_infr, 0) arrests_infraction,
    COALESCE(arrests_violation, 0) arrests_violation,
    COALESCE(arrests_misd,0) arrests_misdemeanor,
    COALESCE(arrests_fel, 0) arrests_felony,
    COALESCE(arrests_other, 0) arrests_other
FROM {{ source('nypd_raw_data', 'officers') }}