{{ 
  config(
    alias='regiao_metropolitana_2017',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(REPLACE(nome_regiao_metropolitana,".0","") AS STRING) nome_regiao_metropolitana,
SAFE_CAST(tipo AS STRING) tipo,
SAFE_CAST(subcategoria_metropolitana AS STRING) subcategoria_metropolitana,
SAFE_CAST(REPLACE(id_municipio,".0","") AS STRING) id_municipio,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(legislacao AS STRING) legislacao,
SAFE_CAST(data_legislacao AS DATE) data_legislacao ,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-dev.br_geobr_mapas_staging.regiao_metropolitana_2017 as t