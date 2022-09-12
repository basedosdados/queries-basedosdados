SELECT
SAFE_CAST(ano AS STRING) ano,
SAFE_CAST(indice_medio AS STRING) indice_medio,
SAFE_CAST(indice AS STRING) indice,
SAFE_CAST(var_anual AS STRING) var_anual,
SAFE_CAST(indice_fechamento_anual AS STRING) indice_fechamento_anual
FROM basedosdados-dev.br_fgv_igp_staging.igp_m_ano AS t