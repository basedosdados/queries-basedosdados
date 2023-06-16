SELECT
SAFE_CAST(data AS DATE) data,
SAFE_CAST(hora AS TIME) hora,
SAFE_CAST(id_subsistema AS STRING) id_subsistema,
SAFE_CAST(subsistema AS STRING) subsistema,
SAFE_CAST(custo_marginal_operacao AS FLOAT64) custo_marginal_operacao
FROM basedosdados-staging.br_ons_estimativa_custos_staging.custo_marginal_operacao_semi_horario AS t