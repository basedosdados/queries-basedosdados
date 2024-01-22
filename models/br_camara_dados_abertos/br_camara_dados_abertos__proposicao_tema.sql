{{
  config(
    alias = 'proposicao_tema',
    schema='br_camara_dados_abertos',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1935,
        "end": 2024,
        "interval": 1}
    }
    )
}}

SELECT
  SAFE_CAST(ano AS INT64) ano,
  SAFE_CAST(uriProposicao AS STRING) url_proposicao,
  SAFE_CAST(siglaTipo AS STRING) tipo_proposicao,
  SAFE_CAST(numero AS STRING) numero,  
  SAFE_CAST(codTema AS STRING) tema,    
  SAFE_CAST(relevancia AS INT64) relevancia,
FROM basedosdados-staging.br_camara_dados_abertos_staging.proposicao_tema AS t