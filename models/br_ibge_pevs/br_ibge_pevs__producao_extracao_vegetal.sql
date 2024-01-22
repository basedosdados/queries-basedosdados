{{ config(
    alias='producao_extracao_vegetal',
    schema='br_ibge_pevs',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1986,
        "end": 2022,
        "interval": 1}
    },
    cluster_by = ["id_municipio"])}}

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(tipo_produto AS STRING) tipo_produto,
SAFE_CAST(produto AS STRING) produto,
SAFE_CAST(unidade AS STRING) unidade,
SAFE_CAST(quantidade AS INT64) quantidade,
ROUND(SAFE_CAST(valor AS FLOAT64), 4) valor,
FROM basedosdados-staging.br_ibge_pevs_staging.producao_extracao_vegetal AS t

