{{
    config(
        alias="indigenas_taxa_alfabetizacao_grupo_idade_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(munic_pio__c_digo_ as string) id_municipio,
    safe_cast(sexo as string) sexo,
    safe_cast(idade as string) grupo_idade,
    safe_cast(valor as float64) taxa_alfabetizacao,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.indigenas_taxa_alfabetizacao_grupo_idade_municipio`
    as t
where idade not like 'Total'
