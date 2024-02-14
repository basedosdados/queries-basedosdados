{{ config(alias="populacao_grupo_idade_uf", schema="br_ibge_censo_2022") }}
select
    t2.sigla as sigla_uf,
    safe_cast(grupo_de_idade as string) grupo_idade,
    safe_cast(`população` as int64) populacao,
from `basedosdados-staging.br_ibge_censo_2022_staging.populacao_grupo_idade_uf ` as t
left join
    `basedosdados.br_bd_diretorios_brasil.uf` t2 on t.unidade_da_federacao = t2.nome
