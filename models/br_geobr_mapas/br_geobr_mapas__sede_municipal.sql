{{ 
  config(
    alias='sede_municipal',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(REPLACE(id_municipio,".0","") AS STRING) id_municipio,
INITCAP(nome_municipio) nome_municipio,
SAFE_CAST(REPLACE(id_uf,".0","") AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_regiao AS STRING) id_regiao,
SAFE_CAST(regiao AS STRING) regiao,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.sede_municipal as t