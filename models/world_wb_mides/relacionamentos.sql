SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_empenho AS STRING) id_empenho,
SAFE_CAST(id_licitacao AS STRING) id_licitacao,
SAFE_CAST(id_municipio AS STRING) id_municipio
FROM basedosdados-staging.world_wb_mides_staging.relacionamentos AS t