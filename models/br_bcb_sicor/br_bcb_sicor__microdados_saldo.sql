{{
  config(
    alias = 'microdados_saldo',
    schema='br_bcb_sicor',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2013,
        "end": 2024,
        "interval": 1}
    },
    cluster_by = ["mes"]
  )
}}

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(numero_ordem AS STRING) numero_ordem,
SAFE_CAST(id_situacao_operacao AS STRING) id_situacao_operacao,
SAFE_CAST(valor_medio_diario AS FLOAT64) valor_medio_diario,
SAFE_CAST(valor_medio_diario_vincendo AS FLOAT64) valor_medio_diario_vincendo,
SAFE_CAST(valor_ultimo_dia AS FLOAT64) valor_ultimo_dia
FROM basedosdados-staging.br_bcb_sicor_staging.microdados_saldo AS t