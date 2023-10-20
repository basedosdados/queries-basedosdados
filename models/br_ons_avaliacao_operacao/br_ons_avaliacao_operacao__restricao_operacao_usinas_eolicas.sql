{{ config(
    alias='restricao_operacao_usinas_eolicas', 
    schema='br_ons_avaliacao_operacao',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2021,
        "end": 2024,
        "interval": 1}
     }) 
}}
SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(hora AS TIME) hora,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(id_ons AS STRING) id_ons,
SAFE_CAST(id_empreendimento_aneel AS STRING) id_empreendimento_aneel,
SAFE_CAST(usina AS STRING) usina,
SAFE_CAST(tipo_razao_restricao AS STRING) tipo_razao_restricao,
SAFE_CAST(tipo_origem_restricao AS STRING) tipo_origem_restricao,
SAFE_CAST(geracao AS FLOAT64) geracao,
SAFE_CAST(geracao_limitada AS FLOAT64) geracao_limitada,
SAFE_CAST(disponibilidade AS FLOAT64) disponibilidade,
SAFE_CAST(geracao_referencia AS FLOAT64) geracao_referencia,
SAFE_CAST(geracao_referencia_final AS FLOAT64) geracao_referencia_final
FROM basedosdados-staging.br_ons_avaliacao_operacao_staging.restricao_operacao_usinas_eolicas AS t