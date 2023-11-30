{{
  config(
    alias = 'votacao_orientacao_bancada',
    schema='br_camara_dados_abertos',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2003,
        "end": 2023,
        "interval": 1}
    },   
  )
}}
SELECT 
  SAFE_CAST(ano AS INT64) ano,
  SAFE_CAST(id_votacao AS STRING) id_votacao,
  SAFE_CAST(sigla_orgao AS STRING) sigla_orgao,
  SAFE_CAST(descricao AS STRING) descricao,
  SAFE_CAST(id_proposicao AS STRING) sigla_bancada,
  SAFE_CAST(orientacao AS STRING) orientacao,
FROM basedosdados-staging.br_camara_dados_abertos_staging.votacao_orientacao_bancada AS t