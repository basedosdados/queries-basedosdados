{{ config(alias='dicionario', schema='br_mg_belohorizonte_smfa_iptu') }}


SELECT 
SAFE_CAST(id_tabela AS STRING) id_tabela,
SAFE_CAST(nome_coluna AS STRING) nome_coluna,
SAFE_CAST(chave AS STRING) chave,
SAFE_CAST(cobertura_temporal AS STRING) cobertura_temporal,
REPLACE(REPLACE(valor, '(Zona de Especial Interesse Social - 2', 'Zona de Especial Interesse Social - 2'), '(Zona de Preservação Ambiental', 'Zona de Preservação Ambiental') AS valor
FROM basedosdados-staging.br_mg_belohorizonte_smfa_iptu_staging.dicionario AS t