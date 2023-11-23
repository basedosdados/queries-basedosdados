{{ config(
    alias='soy_beans_storage_facilities', 
    schema='br_trase_supply_chain') 
}}

SELECT
SAFE_CAST(the_geom AS STRING) geom_id,
SAFE_CAST(cartodb_id AS STRING) cartodb_id,
SAFE_CAST(the_geom_webmercator AS STRING) geom_webmercator_id,
SAFE_CAST(geocode AS STRING) municipality_id,
SAFE_CAST(uf AS STRING) state,
CASE
    WHEN LENGTH(cnpj) = 18 THEN REGEXP_REPLACE(cnpj, r'[^0-9]', '')
    ELSE CONCAT('***', SUBSTR(cnpj, 4, LENGTH(cnpj) - 6), '***')
  END AS cnpj_cpf ,
SAFE_CAST(company AS STRING) company,
SAFE_CAST(capacity AS INT64) capacity,
SAFE_CAST(ST_GEOGPOINT(SAFE_CAST(long AS FLOAT64),SAFE_CAST(lat AS FLOAT64)) as GEOGRAPHY) point,
SAFE_CAST(SAFE.PARSE_DATE("%Y-%m-%d", date) AS DATE) date,
SAFE_CAST(subclass AS STRING) subclass,
SAFE_CAST(dt AS STRING) dt

FROM basedosdados-staging.br_trase_supply_chain_staging.soy_beans_storage_facilities AS t
