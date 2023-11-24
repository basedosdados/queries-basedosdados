{{ config(
    alias='soy_beans_refining_facilities', 
    schema='br_trase_supply_chain') 
}}


SELECT
SAFE_CAST(year AS INT64) year,
SAFE_CAST(the_geom AS STRING) geom_id,
SAFE_CAST(cartodb_id AS STRING) cartodb_id,
SAFE_CAST(the_geom_webmercator AS STRING) geom_webmercator_id,
SAFE_CAST(geocode AS STRING) municipality_id,
SAFE_CAST(state AS STRING) state,
SAFE_CAST(REPLACE(capacity, 'NA', '') AS INT64) capacity,
SAFE_CAST(company AS STRING) company,
SAFE_CAST(capacity AS INT64) capacity,
SAFE_CAST(ST_GEOGPOINT(SAFE_CAST(long AS FLOAT64),SAFE_CAST(lat AS FLOAT64)) as GEOGRAPHY) point
FROM basedosdados-staging.br_trase_supply_chain_staging.soy_beans_refining_facilities AS t