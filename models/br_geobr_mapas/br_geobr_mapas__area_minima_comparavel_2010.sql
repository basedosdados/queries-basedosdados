{{ 
  config(
    alias='area_minima_comparavel_2010',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(REPLACE(id_amc,".0","") AS STRING) id_amc,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.area_minima_comparavel_2010 as t