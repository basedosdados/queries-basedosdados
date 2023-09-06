{{ 
  config(    
    schema='br_bd_diretorios_brasil',
    materialized='table',)
}}
SELECT 
SAFE_CAST(id_regiao_metropolitana AS STRING) id_regiao_metropolitana,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(id_recorte_metropolitano AS STRING) id_recorte_metropolitano,
SAFE_CAST(nome_recorte_metropolitano AS STRING) nome_recorte_metropolitano,
SAFE_CAST(id_subcategoria_metropolitana AS STRING) id_subcategoria_metropolitana,
SAFE_CAST(nome_subcategoria_metropolitana AS STRING) nome_subcategoria_metropolitana,
SAFE_CAST(tipo AS string) tipo,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(nome_regiao AS STRING) nome_regiao,
FROM basedosdados-staging.br_bd_diretorios_brasil_staging.regiao_metropolitana AS t