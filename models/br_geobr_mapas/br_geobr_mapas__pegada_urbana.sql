{{ 
  config(
    alias='pegada_urbana',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_pegada_urbana AS STRING) id_pegada_urbana,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(densidade AS STRING) densidade,
SAFE_CAST(tipo AS STRING) tipo,
SAFE_CAST(area AS FLOAT64) area,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.pegada_urbana as t