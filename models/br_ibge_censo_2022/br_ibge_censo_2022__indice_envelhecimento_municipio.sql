{{ config(alias="indice_envelhecimento_municipio", schema="br_ibge_censo_2022") }}
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
                replace(indice_de_envelhecimento_razao_, ",", ".") as float64
            ) indice_envelhecimento,
            safe_cast(replace(idade_mediana_anos_, ",", ".") as float64) idade_mediana,
            safe_cast(replace(razao_de_sexo_razao_, ",", ".") as float64) razao_sexo,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.indice_envelhecimento_municipio` t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
