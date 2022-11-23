SELECT
SAFE_CAST(id_pessoa AS STRING) id_pessoa,
SAFE_CAST(data_inicio AS DATE) data_inicio,
SAFE_CAST(data_fim AS DATE) data_fim,
SAFE_CAST(equipe AS STRING) equipe,
SAFE_CAST(nivel AS STRING) nivel,
SAFE_CAST(cargo AS STRING) cargo
FROM basedosdados-staging.br_bd_indicadores_staging.equipes AS t