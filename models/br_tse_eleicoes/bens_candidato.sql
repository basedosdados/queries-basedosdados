SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(tipo_eleicao AS STRING) tipo_eleicao,
SAFE_CAST(sequencial_candidato AS STRING) sequencial_candidato,
SAFE_CAST(id_candidato_bd AS STRING) id_candidato_bd,
SAFE_CAST(id_tipo_item AS STRING) id_tipo_item,
SAFE_CAST(tipo_item AS STRING) tipo_item,
SAFE_CAST(descricao_item AS STRING) descricao_item,
SAFE_CAST(valor_item AS FLOAT64) valor_item
FROM basedosdados-staging.br_tse_eleicoes_staging.bens_candidato AS t
