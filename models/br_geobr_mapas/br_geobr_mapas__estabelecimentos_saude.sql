{{ 
  config(
    alias='estabelecimentos_saude',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_cnes AS STRING) id_cnes,
SAFE.ST_GEOGFROMTEXT(geometria) geometria
FROM basedosdados-staging.br_geobr_mapas_staging.estabelecimentos_saude AS t