{{ 
  config(
    alias='cnae_2_3_subclasses',    
    schema='br_bd_diretorios_brasil',
    materialized='table',)
}}
SELECT 
  SAFE_CAST(cnae_2_3 AS STRING) AS cnae_2_3_subclasses,
  SAFE_CAST(descricao AS STRING) AS descricao,
  SAFE_CAST(cnae_2 AS STRING) AS cnae_2,
  SAFE_CAST(descricao_cane_2 AS STRING) AS descricao_cnae_2,
  SAFE_CAST(grupo AS STRING) AS grupo,
  SAFE_CAST(descricao_grupo AS STRING) AS descricao_grupo,
  SAFE_CAST(divisao AS STRING) AS divisao,
  SAFE_CAST(descricao_divisao AS STRING) AS descricao_divisao,
  SAFE_CAST(secao AS STRING) AS secao,
  SAFE_CAST(descricao_secao AS STRING) AS descricao_secao
FROM basedosdados-staging.br_bd_diretorios_brasil_staging.cnae_2_3_subclasses AS t