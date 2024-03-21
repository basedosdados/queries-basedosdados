{{
    config(
        alias="uf",
        schema="br_mme_consumo_energia_eletrica",
        materialized="table",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(tipo_consumo as string) tipo_consumo,
    safe_cast(numero_consumidores as float64) numero_consumidores,
    safe_cast(consumo as float64) consumo
from `basedosdados-staging.br_mme_consumo_energia_eletrica_staging.uf` as t
