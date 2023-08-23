SELECT 
SAFE_CAST(data AS DATE) data,
SAFE_CAST(valor AS FLOAT64) valor,
FROM basedosdados-staging.br_bcb_taxa_selic_staging.taxa_selic AS t