{{
  config(
    alias = 'microdados_liberacao',
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
SAFE_CAST(data_liberacao AS DATE) data_liberacao,
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(numero_ordem AS STRING) numero_ordem,
SAFE_CAST(valor_liberado AS FLOAT64) valor_liberado
FROM basedosdados-staging.br_bcb_sicor_staging.microdados_liberacao AS t