{{
    config(
        schema="br_bd_diretorios_brasil",
        alias="pais",
        materialized="table",
    )
}}

select
    safe_cast(id_pais as string) id_pais,
    safe_cast(nome as string) nome,
    safe_cast(nacionalidade as string) nacionalidade
from `basedosdados-staging.br_bd_diretorios_brasil_staging.pais` as t
