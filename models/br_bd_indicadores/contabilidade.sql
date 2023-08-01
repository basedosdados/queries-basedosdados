SELECT
SAFE_CAST(SAFE_CAST(ano_competencia AS NUMERIC) AS INT64) ano_competencia,
SAFE_CAST(SAFE_CAST(mes_competencia AS NUMERIC) AS INT64) mes_competencia,
SAFE_CAST(SAFE_CAST(ano_caixa AS NUMERIC) AS INT64) ano_caixa,
SAFE_CAST(SAFE_CAST(mes_caixa AS NUMERIC) AS INT64) mes_caixa,
SAFE_CAST(categoria AS STRING) categoria,
SAFE_CAST(tipo AS STRING) tipo,
SAFE_CAST(frequencia AS STRING) frequencia,
SAFE_CAST(equipe AS STRING) equipe,
SAFE_CAST(SAFE_CAST(valor AS NUMERIC) AS FLOAT64) valor
FROM basedosdados-dev.br_bd_indicadores_staging.contabilidade AS t