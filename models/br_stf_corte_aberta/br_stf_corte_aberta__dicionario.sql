{{ config(alias='dicionario', schema='br_stf_corte_aberta') }}

SELECT 
SAFE_CAST(id_tabela AS STRING) id_tabela,
SAFE_CAST(nome_coluna AS STRING) nome_coluna,
INITCAP(chave) chave,
SAFE_CAST(cobertura_temporal AS STRING) cobertura_temporal,
INITCAP(valor) valor
FROM basedosdados-staging.br_stf_corte_aberta_staging.dicionario AS t