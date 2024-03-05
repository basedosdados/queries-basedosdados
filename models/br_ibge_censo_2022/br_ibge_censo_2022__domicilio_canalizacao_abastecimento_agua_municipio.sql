{{
    config(
        alias="domicilio_canalizacao_abastecimento_agua_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(
        principal_forma_de_abastecimento_de_agua as string
    ) tipo_abastecimento_agua,
    safe_cast(existencia_de_canalizacao_de_agua as string) tipo_canalizacao_agua,
    safe_cast(
        domicilios_particulares_permanentes_ocupados_unidades_ as int64
    ) domicilios,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.domicilio_canalizacao_abastecimento_agua_municipio`
    as t
