{{
    config(
        alias="domicilio_quantidade_esgotamento_sanitario_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(tipo_de_esgotamento_sanitario as string) tipo_esgotamento_sanitario,
    safe_cast(
        existencia_de_banheiro_ou_sanitario_e_numero_de_banheiros_de_uso_exclusivo_do_domicilio
        as string
    ) tipo_quantidade_banheiro,
    safe_cast(
        domicilios_particulares_permanentes_ocupados_unidades_ as int64
    ) domicilios,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.domicilio_quantidade_esgotamento_sanitario_municipio`
    as t
