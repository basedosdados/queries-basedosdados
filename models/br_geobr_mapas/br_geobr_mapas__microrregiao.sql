{{ 
  config(
    alias='microrregiao',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}

SELECT 
SAFE_CAST(id_uf AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_microrregiao AS STRING) id_microrregiao,
SAFE.ST_GEOGFROMTEXT(geometria) geometria
FROM basedosdados-staging.br_geobr_mapas_staging.microrregiao AS t