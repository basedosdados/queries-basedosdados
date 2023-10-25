{{
  config(
    alias = 'votacao_objeto',
    schema='br_camara_dados_abertos',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1935,
        "end": 2022,
        "interval": 1}
    },    
  )
}}
SELECT 
  SAFE_CAST(ano AS INT64) ano,
  SAFE_CAST(id_votacao AS STRING) id_votacao,
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(descricao AS STRING) descricao,
  SAFE_CAST(id_proposicao AS STRING) id_proposicao,
  SAFE_CAST(REPLACE(ano_proposicao, ".0", "") AS INT64) ano_proposicao,
  SAFE_CAST(ementa AS STRING) ementa,
  SAFE_CAST(codigo_tipo AS STRING) codigo_tipo,
  SAFE_CAST(sigla_tipo AS STRING) sigla_tipo,
  SAFE_CAST(numero AS STRING) numero,
  SAFE_CAST(titulo AS STRING) titulo
FROM basedosdados-staging.br_camara_dados_abertos_staging.votacao_objeto AS t