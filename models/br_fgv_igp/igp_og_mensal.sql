SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(indice AS FLOAT64) indice,
SAFE_CAST(var_mensal AS FLOAT64) var_mensal,
SAFE_CAST(var_12_meses AS FLOAT64) var_12_meses,
SAFE_CAST(acum_ano AS FLOAT64) acum_ano,
SAFE_CAST(indice_fechamento_mensal AS FLOAT64) indice_fechamento_mensal
FROM basedosdados-dev.br_fgv_igp_staging.igp_og_mes AS t