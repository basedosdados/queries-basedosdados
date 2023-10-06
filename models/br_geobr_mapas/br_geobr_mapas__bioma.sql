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
SAFE_CAST(id_bioma AS STRING) id_bioma,
SAFE_CAST(nome_bioma AS STRING) nome_bioma,
SAFE.ST_GEOGFROMTEXT(geometria) geometria
from basedosdados-staging.br_geobr_mapas_staging.bioma as t