{{ 
  config(
    alias='bioma',
    schema='br_geobr_mapas',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2004,
        "end": 2019,
        "interval": 15}
    },
    )
 }}
SELECT 
SAFE_CAST(year AS INT64) ano,
SAFE_CAST(code_biome AS STRING) id_bioma,
SAFE_CAST(name_biome AS STRING) nome_bioma,
SAFE.ST_GEOGFROMTEXT(geometry) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.bioma as t