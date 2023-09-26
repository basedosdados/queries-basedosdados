{{ 
  config(
    alias='regiao',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_regiao AS STRING) id_regiao,
SAFE_CAST(nome_regiao AS STRING) nome_regiao,
SAFE.ST_GEOGFROMTEXT(geometria) geometria
FROM basedosdados-staging.br_geobr_mapas_staging.regiao AS t