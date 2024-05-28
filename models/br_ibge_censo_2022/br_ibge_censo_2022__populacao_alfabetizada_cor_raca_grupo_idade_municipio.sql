{{
    config(
        alias="populacao_alfabetizada_cor_raca_grupo_idade_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(munic_pio__c_digo_ as string) id_municipio,
    safe_cast(cor_ou_ra_a as string) cor_raca,
    safe_cast(sexo as string) sexo,
    safe_cast(idade as string) grupo_idade,
    safe_cast(alfabetiza__o as string) alfabetizacao,
    safe_cast(valor as int64) populacao,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.populacao_alfabetizada_cor_raca_grupo_idade_municipio`
    as t
