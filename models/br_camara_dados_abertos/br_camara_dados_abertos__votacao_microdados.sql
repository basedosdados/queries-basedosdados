{{
  config(
    alias = 'votacao_microdados',
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
  TIME(TIMESTAMP(horario)) AS horario,
  SAFE_CAST(id_orgao AS STRING) id_orgao,
  SAFE_CAST(sigla_orgao AS STRING) sigla_orgao,
  SAFE_CAST(id_evento AS STRING) id_evento,
  SAFE_CAST(REPLACE(aprovacao, ".0", "") AS INT64) aprovacao,
  SAFE_CAST(voto_sim AS INT64) voto_sim,
  SAFE_CAST(voto_nao AS INT64) voto_nao,
  SAFE_CAST(voto_outro AS INT64) voto_outro,
  SAFE_CAST(descricao AS STRING) descricao,
  SAFE_CAST(data_hora_ultima_votacao AS DATETIME) data_hora_ultima_votacao,  
  SAFE_CAST(descricao_ultima_votacao AS STRING) descricao_ultima_votacao,  
  SAFE_CAST(data_hora_ultima_proposicao AS DATETIME) data_hora_ultima_proposicao,
  SAFE_CAST(descricao_ultima_proposicao AS STRING) descricao_ultima_proposicao,
  SAFE_CAST(id_ultima_proposicao AS STRING) id_ultima_proposicao,
FROM basedosdados-staging.br_camara_dados_abertos_staging.votacao_microdados AS t
