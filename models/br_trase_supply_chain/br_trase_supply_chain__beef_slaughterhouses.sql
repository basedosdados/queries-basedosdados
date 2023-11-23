{{ config(
    alias='beef_slaughterhouses', 
    schema='br_trase_supply_chain') 
}}

SELECT
SAFE_CAST(the_geom AS STRING) geom_id,
SAFE_CAST(cartodb_id AS STRING) cartodb_id,
SAFE_CAST(the_geom_webmercator AS STRING) geom_webmercator_id,
SAFE_CAST(geocode AS STRING) municipality_id,
SAFE_CAST(state AS STRING) state,
SAFE_CAST(address AS STRING) address,
SAFE_CAST(id AS STRING) slaugtherhouse_id,
SAFE_CAST(company AS STRING) company,
SAFE_CAST(other_names AS STRING) other_company_names,
SAFE_CAST(multifunctions AS STRING) multifunctions,
SAFE_CAST(resolution AS STRING) resolution_id,
SAFE_CAST(subclass AS STRING) subclass,
SAFE_CAST(inspection_level AS STRING) inspection_level,
SAFE_CAST(REPLACE(inspection_number, 'NA', '') AS STRING) inspection_number,
SAFE_CAST(REPLACE(tac, 'NA', '') AS STRING) tac,
SAFE_CAST(REGEXP_REPLACE(status, r'(?i)^NA$', '') AS STRING) status,
SAFE_CAST(FORMAT_DATE('%Y-%m-%d', SAFE.PARSE_DATE('%d/%m/%Y', date_sif_registered)) AS STRING) date_sif_registered,
SAFE_CAST(REPLACE(sif_category,'NA','') AS STRING) sif_category,
SAFE_CAST(ST_GEOGPOINT(SAFE_CAST(long AS FLOAT64),SAFE_CAST(lat AS FLOAT64)) as GEOGRAPHY) point
FROM basedosdados-staging.br_trase_supply_chain_staging.beef_slaughterhouses AS t