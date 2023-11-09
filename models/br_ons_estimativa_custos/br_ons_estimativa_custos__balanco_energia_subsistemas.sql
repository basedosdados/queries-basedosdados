{{ config(
    alias='balanco_energia_subsistemas', 
    schema='br_ons_estimativa_custos') 
}}

SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(hora AS TIME) hora,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(geracao_hidraulica_verificada AS FLOAT64) geracao_hidraulica_verificada,
SAFE_CAST(geracao_termica_verificada AS FLOAT64) geracao_termica_verificada,
SAFE_CAST(geracao_eolica_verificada AS FLOAT64) geracao_eolica_verificada,
SAFE_CAST(geracao_fotovoltaica_verificada AS FLOAT64) geracao_fotovoltaica_verificada,
SAFE_CAST(carga_verificada AS FLOAT64) carga_verificada,
SAFE_CAST(intercambio_verificado AS FLOAT64) intercambio_verificado
FROM basedosdados-staging.br_ons_estimativa_custos_staging.balanco_energia_subsistemas AS t