{{
    config(
        schema="br_bd_diretorios_brasil",
        materialized="table",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
    )
}}

select
    safe_cast(lpad(cep, 8, '0') as string) cep,
    safe_cast(logradouro as string) logradouro,
    safe_cast(complemento as string) complemento,
    safe_cast(bairro as string) bairro,
    safe_cast(cidade as string) cidade,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    st_geogpoint(
        safe_cast(longitude as float64), safe_cast(latitude as float64)
    ) centroide
from `basedosdados-staging.br_bd_diretorios_brasil_staging.cep ` as t
