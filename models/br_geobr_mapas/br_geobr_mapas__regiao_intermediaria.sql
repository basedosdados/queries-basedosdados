{{ 
  config(
    alias='regiao_intermediaria',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_uf AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_regiao_intermediaria AS STRING) id_regiao_intermediaria,
SAFE.ST_GEOGFROMTEXT(geometria) geometria
FROM basedosdados-staging.br_geobr_mapas_staging.regiao_intermediaria AS t