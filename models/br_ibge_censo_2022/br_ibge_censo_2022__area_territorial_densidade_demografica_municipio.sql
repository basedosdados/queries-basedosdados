{{
    config(
        alias="area_territorial_densidade_demografica_municipio",
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
            safe_cast(populacao_residente_pessoas_ as int64) populacao_residente,
            safe_cast(
                area_da_unidade_territorial_quilometros_quadrados_ as int64
            ) area_unidade_territorial,
        # SAFE_CAST(REPLACE(densidade_demografica_habitante_por_quilometro_quadrado_,
        # ",", ".") AS FLOAT64) densidade_demografica,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.area_territorial_densidade_demografica_municipio`
            as t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
