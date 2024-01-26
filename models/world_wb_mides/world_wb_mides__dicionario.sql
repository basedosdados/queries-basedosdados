{{
  config(
    alias = 'dicionario',
    schema = "world_wb_mides",
    materialized = "table",
    labels = {"tema": "economia"}
  )
 }}
-- atualiza novas chaves
SELECT 
SAFE_CAST(id_tabela AS STRING) id_tabela,
SAFE_CAST(coluna AS STRING) coluna,
SAFE_CAST(chave AS STRING) chave,
SAFE_CAST(cobertura_temporal AS STRING) cobertura_temporal,
SAFE_CAST(valor AS STRING) valor
FROM basedosdados-staging.world_wb_mides_staging.dicionario AS t

