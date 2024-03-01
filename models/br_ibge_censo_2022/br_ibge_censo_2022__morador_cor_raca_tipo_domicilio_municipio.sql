{{
    config(
        alias="morador_cor_raca_tipo_domicilio_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(tipo_de_domicilio as string) tipo_domicilio,
    safe_cast(grupo_de_idade as string) grupo_idade,
    safe_cast(cor_ou_raca as string) cor_raca,
    safe_cast(
        moradores_em_domicilios_particulares_permanentes_ocupados_pessoas_ as int64
    ) moradores,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.morador_cor_racao_tipo_domicilio_municipio`
    as t
