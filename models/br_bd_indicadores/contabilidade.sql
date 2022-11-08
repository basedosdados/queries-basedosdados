SELECT
SAFE_CAST(ano_competencia AS INT64) ano_competencia,
SAFE_CAST(mes_competencia AS INT64) mes_competencia,
SAFE_CAST(ano_caixa AS INT64) ano_caixa,
SAFE_CAST(mes_caixa AS INT64) mes_caixa,
SAFE_CAST(categoria AS STRING) categoria,
SAFE_CAST(tipo AS STRING) tipo,
SAFE_CAST(frequencia AS STRING) frequencia,
SAFE_CAST(valor AS FLOAT64) valor
FROM basedosdados-staging.br_bd_indicadores_staging.contabilidade AS t