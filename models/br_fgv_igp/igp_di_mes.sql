SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(indice AS FLOAT64) indice,
SAFE_CAST(variacao_mensal AS FLOAT64) variacao_mensal,
SAFE_CAST(variacao_12_meses AS FLOAT64) variacao_12_meses,
SAFE_CAST(variacao_acumulada_ano AS FLOAT64) variacao_acumulada_ano,
SAFE_CAST(indice_fechamento_mensal AS FLOAT64) indice_fechamento_mensal
FROM basedosdados-staging.br_fgv_igp_staging.igp_di_mes AS t