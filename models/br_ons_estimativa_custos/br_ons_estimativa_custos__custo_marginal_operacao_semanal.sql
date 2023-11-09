{{ config(
    alias='custo_marginal_operacao_semanal', 
    schema='br_ons_estimativa_custos') 
}}
SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(custo_marginal_operacao_semanal AS FLOAT64) custo_marginal_operacao_semanal,
SAFE_CAST(custo_marginal_operacao_semanal_carga_leve AS FLOAT64) custo_marginal_operacao_semanal_carga_leve,
SAFE_CAST(custo_marginal_operacao_semanal_carga_media AS FLOAT64) custo_marginal_operacao_semanal_carga_media,
SAFE_CAST(custo_marginal_operacao_semanal_carga_pesada AS FLOAT64) custo_marginal_operacao_semanal_carga_pesada
FROM basedosdados-staging.br_ons_estimativa_custos_staging.custo_marginal_operacao_semanal AS t