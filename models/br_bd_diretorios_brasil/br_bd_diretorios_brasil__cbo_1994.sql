{{
    config(
        alias="cbo_1994",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(cbo_1994 as string) cbo_1994,
    safe_cast(initcap(descricao) as string) descricao
from `basedosdados-staging.br_bd_diretorios_brasil_staging.cbo_1994` as t
