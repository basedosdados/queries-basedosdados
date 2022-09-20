SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(indice_medio AS FLOAT64) indice_medio,
SAFE_CAST(indice AS FLOAT64) indice,
SAFE_CAST(variacao_anual AS FLOAT64) variacao_anual,
SAFE_CAST(indice_fechamento_anual AS FLOAT64) indice_fechamento_anual
FROM basedosdados-staging.br_fgv_igp_staging.igp_m_ano AS t