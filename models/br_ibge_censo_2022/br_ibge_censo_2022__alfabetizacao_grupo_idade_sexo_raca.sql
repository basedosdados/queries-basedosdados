{{
    config(
        alias="alfabetizacao_grupo_idade_sexo_raca",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(municipio_codigo_ as string) id_municipio,
    safe_cast(cor_ou_raca as string) cor_raca,
    safe_cast(sexo as string) sexo,
    safe_cast(idade as string) grupo_idade,
    safe_cast(alfabetizacao as string) alfabetizacao,
    safe_cast(valor as int64) populacao,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.alfabetizacao_grupo_idade_sexo_raca`
    as t
