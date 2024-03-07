{{
    config(
        alias="quilombolas_populacao_residente_municipio",
        schema="br_ibge_censo_2022",
    )
}}
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
            safe_cast(localizacao_do_domicilio as string) territorios_quilombolas,
            safe_cast(pessoas_quilombolas_pessoas_ as int64) pessoas_quilombolas,
            safe_cast(populacao_residente_pessoas_ as int64) populacao_residente,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.quilombolas_populacao_residente_municipio`
            as t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
