{{ config(alias='mes_brasil', schema='br_ibge_inpc') }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(indice AS FLOAT64) indice,
SAFE_CAST(variacao_mensal AS FLOAT64) variacao_mensal,
SAFE_CAST(variacao_trimestral AS FLOAT64) variacao_trimestral,
SAFE_CAST(variacao_semestral AS FLOAT64) variacao_semestral,
SAFE_CAST(variacao_anual AS FLOAT64) variacao_anual,
SAFE_CAST(variacao_doze_meses AS FLOAT64) variacao_doze_meses
FROM basedosdados-staging.br_ibge_inpc_staging.mes_brasil AS t
