{{ config(alias="populacao_residente_municipio", schema="br_ibge_censo_2022") }}
with
    ibge as (
        select
            municipio,
            safe_cast(
                trim(regexp_extract(municipio, r'([^\(]+)')) as string
            ) nome_municipio,
            safe_cast(
                trim(regexp_extract(municipio, r'\(([^)]+)\)')) as string
            ) sigla_uf,
            safe_cast(forma_de_declaracao_da_idade as string) forma_declaracao_idade,
            safe_cast(sexo as string) sexo,
            safe_cast(idade as string) idade,
            safe_cast(populacao_residente_pessoas_ as int64) populacao_residente,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.populacao_residente_municipio` t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
