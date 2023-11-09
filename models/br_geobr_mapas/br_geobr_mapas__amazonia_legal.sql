{{ 
  config(
    alias='amazonia_legal',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.amazonia_legal as t