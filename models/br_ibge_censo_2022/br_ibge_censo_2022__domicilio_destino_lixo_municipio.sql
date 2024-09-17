{{ config(alias="domicilio_destino_lixo_municipio", schema="br_ibge_censo_2022") }}
select
    safe_cast(replace(ano, ".0", "") as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(destino_do_lixo as string) tipo_destino_lixo,
    safe_cast(
        domicilios_particulares_permanentes_ocupados_unidades_ as int64
    ) domicilios,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.domicilio_destino_lixo_municipio`
    as t
