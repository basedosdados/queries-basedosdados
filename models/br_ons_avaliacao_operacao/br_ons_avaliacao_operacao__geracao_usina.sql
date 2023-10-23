{{ config(
    alias='geracao_usina', 
    schema='br_ons_avaliacao_operacao',
    materialized = 'incremental',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2000,
        "end": 2024,
        "interval": 1}
     }) 
}}
WITH ons as (
SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(hora AS TIME) hora,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(id_empreendimento_aneel AS STRING) id_empreendimento_aneel,
SAFE_CAST(usina AS STRING) usina,
SAFE_CAST(tipo_usina AS STRING) tipo_usina,
SAFE_CAST(tipo_modalidade_operacao AS STRING) tipo_modalidade_operacao,
SAFE_CAST(tipo_combustivel AS STRING) tipo_combustivel,
SAFE_CAST(geracao AS FLOAT64) geracao
FROM basedosdados-staging.br_ons_avaliacao_operacao_staging.geracao_usina AS t
)
SELECT *
FROM ons
{% if is_incremental() %} 
WHERE data > (SELECT max(data) FROM {{ this }} )
{% endif %}