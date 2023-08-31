{{ 
  config(
    schema='br_ms_cnes',
    materialized='incremental',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2007,
        "end": 2023,
        "interval": 1}
     } 
    )
 }}


WITH raw_cnes_leito AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.leito`
  WHERE CNES IS NOT NULL),
cnes_leito_without_duplicates AS (
    SELECT DISTINCT *
    FROM raw_cnes_leito
)

SELECT 
SAFE_CAST(ano AS INT64) AS ano,
SAFE_CAST(mes AS INT64) AS mes,
SAFE_CAST(sigla_uf AS STRING) AS sigla_uf,
SAFE_CAST(CNES AS STRING) AS id_estabelecimento_cnes,
SAFE_CAST(CODLEITO AS STRING) AS tipo_especialidade_leito,
SAFE_CAST(TP_LEITO AS STRING) AS tipo_leito,
SAFE_CAST(QT_EXIST AS STRING) AS quantidade_total,
SAFE_CAST(QT_CONTR AS STRING) AS quantidade_contratado,
SAFE_CAST(QT_SUS AS STRING) AS quantidade_sus
FROM cnes_leito_without_duplicates
