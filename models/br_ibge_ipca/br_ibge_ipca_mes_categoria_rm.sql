{{ config(alias='mes_categoria_rm', schema='br_ibge_ipca') }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_regiao_metropolitana AS STRING) id_regiao_metropolitana,
SAFE_CAST(id_categoria AS STRING) id_categoria,
SAFE_CAST(id_categoria_bd AS STRING) id_categoria_bd,
SAFE_CAST(categoria AS STRING) categoria,
SAFE_CAST(peso_mensal AS FLOAT64) peso_mensal,
SAFE_CAST(variacao_mensal AS FLOAT64) variacao_mensal,
SAFE_CAST(variacao_anual AS FLOAT64) variacao_anual,
SAFE_CAST(variacao_doze_meses AS FLOAT64) variacao_doze_meses
FROM basedosdados-staging.br_ibge_ipca_staging.mes_categoria_rm AS t