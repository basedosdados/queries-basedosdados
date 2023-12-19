{{ config(alias='populacao_grupos_idade_uf',schema='br_ibge_censo_2022') }}
SELECT
t2.sigla as sigla_uf,
SAFE_CAST(grupo_de_idade AS STRING) grupo_idade,
SAFE_CAST(`população` AS INT64) populacao,
FROM basedosdados-dev.br_ibge_censo_2022_staging.populacao_grupos_idade_uf AS t
left join `basedosdados.br_bd_diretorios_brasil.uf` t2
on t.unidade_da_federacao = t2.nome



