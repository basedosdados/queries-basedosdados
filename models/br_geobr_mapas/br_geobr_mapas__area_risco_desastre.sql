{{ 
  config(
    alias='area_risco_desastre',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(REPLACE(geocodigo_bater,".0","") AS STRING) geocodigo_bater,
SAFE_CAST(origem AS STRING) origem,
SAFE_CAST(acuracia AS STRING) acuracia,
SAFE_CAST(observacao AS STRING) observacao,
SAFE_CAST(quantidade_poligono AS INT64) quantidade_poligono,
SAFE_CAST(REPLACE(id_municipio,".0","") AS STRING) id_municipio,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.area_risco_desastre as t