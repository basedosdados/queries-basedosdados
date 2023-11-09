{{ config(
    alias='geracao_termica_motivo_despacho', 
    schema='br_ons_avaliacao_operacao',
    materialized = 'incremental',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2013,
        "end": 2024,
        "interval": 1}
     },
    cluster_by=['ano', 'mes']) 
}}

WITH ons as (
SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(hora AS TIME) hora,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(REPLACE(id_usina_planejamento, '.0', '') AS STRING) id_usina_planejamento,
SAFE_CAST(usina AS STRING) usina,
SAFE_CAST(tipo_patamar AS STRING) tipo_patamar,
SAFE_CAST(atendimento_satisfatorio AS INT64) atendimento_satisfatorio,
SAFE_CAST(geracao_programada_total AS FLOAT64) geracao_programada_total,
SAFE_CAST(geracao_programada_ordem_merito AS FLOAT64) geracao_programada_ordem_merito,
SAFE_CAST(geracao_programada_referencia_ordem_merito AS FLOAT64) geracao_programada_referencia_ordem_merito,
SAFE_CAST(geracao_programada_inflexibilidade AS FLOAT64) geracao_programada_inflexibilidade,
SAFE_CAST(geracao_programada_razao_eletrica AS FLOAT64) geracao_programada_razao_eletrica,
SAFE_CAST(geracao_programada_seguranca_energetica AS FLOAT64) geracao_programada_seguranca_energetica,
SAFE_CAST(geracao_programada_sem_ordem_merito AS FLOAT64) geracao_programada_sem_ordem_merito,
SAFE_CAST(geracao_programada_reposicao_perdas AS FLOAT64) geracao_programada_reposicao_perdas,
SAFE_CAST(geracao_programada_exportacao AS FLOAT64) geracao_programada_exportacao,
SAFE_CAST(geracao_programada_reserva_potencia AS FLOAT64) geracao_programada_reserva_potencia,
SAFE_CAST(geracao_programada_substituicao AS FLOAT64) geracao_programada_substituicao,
SAFE_CAST(geracao_programada_unit_commitment AS FLOAT64) geracao_programada_unit_commitment,
SAFE_CAST(geracao_programada_constrained_off AS FLOAT64) geracao_programada_constrained_off,
SAFE_CAST(geracao_verificada AS FLOAT64) geracao_verificada,
SAFE_CAST(ordem_merito_verificada AS FLOAT64) ordem_merito_verificada,
SAFE_CAST(geracao_inflexibilidade_verificada AS FLOAT64) geracao_inflexibilidade_verificada,
SAFE_CAST(geracao_razao_eletrica_verificada AS FLOAT64) geracao_razao_eletrica_verificada,
SAFE_CAST(geracao_seguranca_energetica_verificada AS FLOAT64) geracao_seguranca_energetica_verificada,
SAFE_CAST(geracao_sem_ordem_merito_verificada AS FLOAT64) geracao_sem_ordem_merito_verificada,
SAFE_CAST(geracao_reposicao_perdas_verificada AS FLOAT64) geracao_reposicao_perdas_verificada,
SAFE_CAST(geracao_exportacao_verificada AS FLOAT64) geracao_exportacao_verificada,
SAFE_CAST(geracao_reserva_potencia_verificada AS FLOAT64) geracao_reserva_potencia_verificada,
SAFE_CAST(geracao_substituicao_verificada AS FLOAT64) geracao_substituicao_verificada,
SAFE_CAST(geracao_unit_commitment_verificada AS FLOAT64) geracao_unit_commitment_verificada,
SAFE_CAST(geracao_constrained_off_verificada AS FLOAT64) geracao_constrained_off_verificada
FROM basedosdados-staging.br_ons_avaliacao_operacao_staging.geracao_termica_motivo_despacho AS t
)
SELECT *
FROM ons
{% if is_incremental() %} 
WHERE data > (SELECT max(data) FROM {{ this }} )
{% endif %}