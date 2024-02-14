{{
    config(
        alias="indigenas_domicilio_pelo_menos_um_morador_indigena_terra_indigena",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(cod_ as string) id_terra_indigena,
    safe_cast(
        trim(
            regexp_extract(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')
        ) as string
    ) terra_indigena,
    safe_cast(
        trim(
            regexp_extract(terra_indigena_por_unidade_da_federacao, r'\(([^)]+)\)')
        ) as string
    ) sigla_uf,
    safe_cast(
        domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_domicilios_
        as int64
    ) domicilios,
    safe_cast(
        moradores_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_pessoas_
        as int64
    ) moradores,
    safe_cast(
        moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_pessoas_
        as int64
    ) moradores_indigenas,
# SAFE_CAST(REPLACE(media_de_moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_pessoas_, ",", ".") AS FLOAT64) media_moradores_indigenas_domicilios_terras_indigenas_pelo_menos_um,
from
    basedosdados
    - staging.br_ibge_censo_2022_staging.indigenas_domicilio_pelo_menos_um_morador_indigena_terra_indigena
    as t
