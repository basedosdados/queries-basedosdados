{{
    config(
        alias="distrito_1991",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_distrito as string) id_distrito,
    safe_cast(nome as string) nome,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf
from `basedosdados-staging.br_bd_diretorios_brasil_staging.distrito`
where ano = '1991'