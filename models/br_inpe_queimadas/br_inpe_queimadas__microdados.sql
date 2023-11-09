{{
  config(
    alias = 'microdados',
    schema = "br_inpe_queimadas",
    materialized = "table",
    labels = {"tema": "meio-ambiente"}
  )
 }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(bioma AS STRING) bioma,
SAFE_CAST(id_bdq AS STRING) id_bdq,
SAFE_CAST(id_foco AS STRING) id_foco,
SAFE_CAST(data_hora AS TIME) data_hora,
ST_GEOGPOINT(SAFE_CAST (longitude AS FLOAT64), SAFE_CAST (latitude AS FLOAT64)) centroide,
FROM basedosdados-staging.br_inpe_queimadas_staging.microdados AS t

