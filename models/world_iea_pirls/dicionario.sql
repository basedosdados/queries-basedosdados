{{ config(alias='dicionario', schema='world_iea_pirls') }}

SELECT 
SAFE_CAST(id_tabela AS STRING) id_tabela,
SAFE_CAST(coluna AS STRING) coluna,
SAFE_CAST(chave AS STRING) chave,
SAFE_CAST(cobertura_temporal AS STRING) cobertura_temporal,
SAFE_CAST(valor AS STRING) valor
FROM basedosdados-staging.world_iea_pirls_staging.dicionario AS t
