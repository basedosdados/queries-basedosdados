{{
    config(
        alias="domicilio_recenseado_especie_municipio", schema="br_ibge_censo_2022"
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
            safe_cast(especie as string) especie,
            safe_cast(domicilios_recenseados_domicilios_ as int64) domicilios,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.domicilio_recenseado_especie_municipio`
            as t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
