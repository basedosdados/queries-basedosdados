{{ config(
    alias='balanco_energia_subsistemas_dessem', 
    schema='br_ons_estimativa_custos',
    cluster_by=['ano', 'mes']
    ) 
}}

SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(hora AS TIME) hora,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(valor_demanda AS FLOAT64) valor_demanda,
SAFE_CAST(usina_hidraulica_verificada AS FLOAT64) usina_hidraulica_verificada,
SAFE_CAST(geracao_pequena_usina_hidraulica_verificada AS FLOAT64) geracao_pequena_usina_hidraulica_verificada,
SAFE_CAST(geracao_usina_termica_verificada AS FLOAT64) geracao_usina_termica_verificada,
SAFE_CAST(geracao_pequena_usina_termica_verificada AS FLOAT64) geracao_pequena_usina_termica_verificada,
SAFE_CAST(geracao_eolica_verificada AS FLOAT64) geracao_eolica_verificada,
SAFE_CAST(geracao_fotovoltaica_verificada AS FLOAT64) geracao_fotovoltaica_verificada
FROM basedosdados-staging.br_ons_estimativa_custos_staging.balanco_energia_subsistemas_dessem AS t