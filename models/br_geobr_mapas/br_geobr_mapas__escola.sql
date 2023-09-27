{{ 
  config(
    alias='escola',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}

SELECT 
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_escola AS STRING) id_escola,
SAFE.ST_GEOGFROMTEXT(geometria) geometria
FROM basedosdados-staging.br_geobr_mapas_staging.escola AS t