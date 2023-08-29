{{ config(
    alias='cotacoes',
    schema='br_b3_cotacoes',
    materialized='incremental',
    partition_by={
      "field": "data_referencia",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by='acao_atualizacao',
) }}

WITH b3 AS (SELECT 
SAFE_CAST(data_referencia AS DATE) data_referencia,
SAFE_CAST(data_negocio AS DATE) data_negocio,
SAFE_CAST(hora_fechamento AS TIME) hora_fechamento,
SAFE_CAST(codigo_identificador_negocio AS STRING) codigo_identificador_negocio,
SAFE_CAST(codigo_instrumento AS STRING) codigo_instrumento,
SAFE_CAST(codigo_participante_comprador AS STRING) codigo_participante_comprador,
SAFE_CAST(codigo_participante_vendedor AS STRING) codigo_participante_vendedor,
SAFE_CAST(acao_atualizacao AS STRING) acao_atualizacao,
SAFE_CAST(tipo_sessao_pregao AS STRING) tipo_sessao_pregao,
SAFE_CAST(quantidade_negociada AS INT64) quantidade_negociada,
SAFE_CAST(preco_negocio AS FLOAT64) preco_negocio
FROM basedosdados-staging.br_b3_cotacoes_staging.cotacoes AS t)
SELECT * FROM b3

# ----- Select the max(data_referencia) timestamp — the most recent record.
# ----- From {{ this }} — the table for this model as it exists in the warehouse, as built in our last run.
# ----- So max(data_referencia) FROM {{ this }} the most recent record processed in our last run.
{% if is_incremental() %}
WHERE data_referencia > (SELECT max(data_referencia) FROM {{ this }})
{% endif %}