{{
    config(
        alias="indigenas_populacao_grupo_idade_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(grupo_de_idade as string) grupo_idade,
    safe_cast(sexo as string) sexo,
    safe_cast(pessoas_indigenas_pessoas_ as int64) populacao_residente,
from
    basedosdados
    - staging.br_ibge_censo_2022_staging.indigenas_populacao_grupo_idade_municipio as t
