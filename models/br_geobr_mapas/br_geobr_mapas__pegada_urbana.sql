{{ 
  config(
    alias='pegada_urbana',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(REPLACE(id_pegada_urbana,".0","") AS STRING) id_pegada_urbana,
SAFE_CAST(REPLACE(id_municipio,".0","") AS STRING) id_municipio,
SAFE_CAST(densidade AS STRING) densidade,
SAFE_CAST(tipo AS STRING) tipo,
SAFE_CAST(area AS FLOAT64) area,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.pegada_urbana as t