SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(indice AS FLOAT64) indice,
SAFE_CAST(variacao_mensal AS FLOAT64) variacao_mensal
FROM basedosdados-staging.br_sp_saopaulo_dieese_icv_staging.mes as t