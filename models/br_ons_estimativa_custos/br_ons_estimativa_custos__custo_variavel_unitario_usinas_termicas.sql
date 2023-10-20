{{ config(
    alias='custo_variavel_unitario_usinas_termicas', 
    schema='br_ons_estimativa_custos') 
}}
SELECT
SAFE_CAST(data_inicio AS DATE) data_inicio,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(data_fim AS DATE) data_fim,
SAFE_CAST(ano AS INT64) ano_pmo,
SAFE_CAST(mes AS INT64) mes_pmo,
SAFE_CAST(numero_revisao AS INT64) numero_revisao,
SAFE_CAST(semana_operativa AS STRING) semana_operativa,
SAFE_CAST(id_modelo_usina AS STRING) id_modelo_usina,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(usina AS STRING) usina,
SAFE_CAST(custo_variavel_unitario AS FLOAT64) custo_variavel_unitario
FROM basedosdados-staging.br_ons_estimativa_custos_staging.custo_variavel_unitario_usinas_termicas AS t