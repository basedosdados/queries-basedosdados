{{ 
  config(    
    schema='br_bd_diretorios_brasil',
    materialized='table',)
}}
SELECT 
SAFE_CAST(lpad(id_regiao_metropolitana, 5, '0') AS STRING) id_regiao_metropolitana,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(lpad(id_recorte_metropolitano, 3, "0") AS STRING) id_recorte_metropolitano,
SAFE_CAST(nome_recorte_metropolitano AS STRING) nome_recorte_metropolitano,
SAFE_CAST(lpad(id_subcategoria_metropolitana, 7, "0") AS STRING) id_subcategoria_metropolitana,
SAFE_CAST(nome_subcategoria_metropolitana AS STRING) nome_subcategoria_metropolitana,
SAFE_CAST(tipo AS INT64) tipo,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(nome_regiao AS STRING) nome_regiao,
FROM basedosdados-dev.br_bd_diretorios_brasil_staging.regiao_metropolitana AS t