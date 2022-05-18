SELECT 
SAFE_CAST(cnpj AS STRING) cnpj,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(tipo AS STRING) tipo
FROM basedosdados-staging.br_cvm_administradores_carteira_staging.responsavel AS t