{{
  config(
    alias = 'voto_parlamentar',
    schema='br_camara_dados_abertos',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2003,
        "end": 2022,
        "interval": 1}
    },    
  )
}}
SELECT 
  SAFE_CAST(ano AS INT64) ano,
  SAFE_CAST(id_votacao AS STRING) id_votacao,
  SAFE_CAST(data_hora AS DATETIME) data_hora,
  SAFE_CAST(voto AS STRING) voto,
  SAFE_CAST(id_deputado AS STRING) id_deputado,
  SAFE_CAST(nome AS STRING) nome,
  SAFE_CAST(sigla_partido AS STRING) sigla_partido,
  SAFE_CAST(sigla_uf AS STRING) sigla_uf,
  SAFE_CAST(id_legislatura AS STRING) id_legislatura,
FROM basedosdados-staging.br_camara_dados_abertos_staging.voto_parlamentar AS t