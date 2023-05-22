SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(dado AS STRING) dad
FROM basedosdados-staging.test_dataset_staging.test_table AS t