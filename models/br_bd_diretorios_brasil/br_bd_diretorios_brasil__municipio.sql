{{ 
  config(
    alias='municipio',    
    schema='br_bd_diretorios_brasil',
    materialized='table',)
}}
SELECT 
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_municipio_6 AS STRING) id_municipio_6,
SAFE_CAST(id_municipio_tse AS STRING) id_municipio_tse,
SAFE_CAST(id_municipio_rf AS STRING) id_municipio_rf,
SAFE_CAST(id_municipio_bcb AS STRING) id_municipio_bcb,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(capital_uf AS INT64) capital_uf,
SAFE_CAST(id_comarca AS STRING) id_comarca,
SAFE_CAST(id_regiao_saude AS STRING) id_regiao_saude,
SAFE_CAST(nome_regiao_saude AS STRING) nome_regiao_saude,
SAFE_CAST(id_regiao_imediata AS STRING) id_regiao_imediata,
SAFE_CAST(nome_regiao_imediata AS STRING) nome_regiao_imediata,
SAFE_CAST(id_regiao_intermediaria AS STRING) id_regiao_intermediaria,
SAFE_CAST(nome_regiao_intermediaria AS STRING) nome_regiao_intermediaria,
SAFE_CAST(id_microrregiao AS STRING) id_microrregiao,
SAFE_CAST(nome_microrregiao AS STRING) nome_microrregiao,
SAFE_CAST(id_mesorregiao AS STRING) id_mesorregiao,
SAFE_CAST(nome_mesorregiao AS STRING) nome_mesorregiao,
SAFE_CAST(ddd AS STRING) ddd,
SAFE_CAST(id_uf AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(nome_uf AS STRING) nome_uf,
SAFE_CAST(nome_regiao AS STRING) nome_regiao,
SAFE_CAST(amazonia_legal AS INT64) amazonia_legal,
SAFE.ST_GEOGFROMTEXT(centroide) centroide
FROM basedosdados-dev.br_bd_diretorios_brasil_staging.municipio AS t