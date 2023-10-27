{{ 
  config(
    alias='cnae_1',    
    schema='br_bd_diretorios_brasil',
    materialized='table',)
}}
SELECT 
  SAFE_CAST(REPLACE(REPLACE(t.cnae_1, '.', ''), '-', '') AS STRING) AS cnae_1,
  SAFE_CAST(t.descricao AS STRING) AS descricao,
  SAFE_CAST(t.grupo AS STRING) AS grupo,
  SAFE_CAST(t.descricao_grupo AS STRING) AS descricao_grupo,
  SAFE_CAST(t.divisao AS STRING) AS divisao,
  SAFE_CAST(t.descricao_divisao AS STRING) AS descricao_divisao,
  SAFE_CAST(t.secao AS STRING) AS secao,
  SAFE_CAST(t.descricao_secao AS STRING) AS descricao_secao
FROM basedosdados-dev.br_bd_diretorios_brasil_staging.cnae_1 AS t
