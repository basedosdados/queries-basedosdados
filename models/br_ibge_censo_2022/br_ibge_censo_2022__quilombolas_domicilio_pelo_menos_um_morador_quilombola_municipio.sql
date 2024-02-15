{{
    config(
        alias="quilombolas_domicilio_pelo_menos_um_morador_quilombola_municipio",
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
            safe_cast(
                domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_domicilios_
                as int64
            ) domicilios,
            safe_cast(
                moradores_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_
                as int64
            ) moradores,
            safe_cast(
                moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_
                as int64
            ) moradores_quilombolas,
            # SAFE_CAST(media_de_moradores_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_ AS FLOAT64) media_moradores_domicilios_particulares_pelo_menos_um,
            # SAFE_CAST(media_de_moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_ AS FLOAT64) media_moradores_quilombolas_domicilios_particulares_pelo_menos_um,
            safe_cast(localizacao_do_domicilio as string) localizacao_domicilio,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.quilombolas_domicilio_pelo_menos_um_morador_quilombola_municipio`
            as t
    )
select t2.cod as id_municipio, ibge.* except (municipio, nome_municipio, sigla_uf)
from ibge
left join
    `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
    on ibge.municipio = t2.municipio
