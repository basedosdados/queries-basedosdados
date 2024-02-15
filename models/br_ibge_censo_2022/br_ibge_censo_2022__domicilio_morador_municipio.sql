{{ config(alias="domicilio_morador_municipio", schema="br_ibge_censo_2022") }}
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
            safe_cast(
                domicilios_particulares_permanentes_ocupados_domicilios_ as int64
            ) domicilios,
            safe_cast(
                moradores_em_domicilios_particulares_permanentes_ocupados_pessoas_
                as int64
            ) moradores,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.domicilio_morador_municipio` t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
