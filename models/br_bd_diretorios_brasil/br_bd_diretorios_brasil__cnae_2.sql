{{ 
  config(
    alias='cnae_2',    
    schema='br_bd_diretorios_brasil',
    materialized='table',)
}}
SELECT 
  SAFE_CAST(REPLACE(REPLACE(cnae_2, '.', ''), '-', '') AS STRING) AS cnae_2,
  SAFE_CAST(descricao AS STRING) AS descricao,
  SAFE_CAST(grupo AS STRING) AS grupo,
  SAFE_CAST(descricao_grupo AS STRING) AS descricao_grupo,
  SAFE_CAST(divisao AS STRING) AS divisao,
  SAFE_CAST(descricao_divisao AS STRING) AS descricao_divisao,
  SAFE_CAST(secao AS STRING) AS secao,
  SAFE_CAST(descricao_secao AS STRING) AS descricao_secao
FROM basedosdados-staging.br_bd_diretorios_brasil_staging.cnae_2 AS t