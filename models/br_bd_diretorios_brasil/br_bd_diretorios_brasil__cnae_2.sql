{{
    config(
        alias="cnae_2",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(replace(replace(cnae_2, '.', ''), '-', '') as string) as cnae_2,
    safe_cast(descricao as string) as descricao,
    safe_cast(grupo as string) as grupo,
    safe_cast(descricao_grupo as string) as descricao_grupo,
    safe_cast(divisao as string) as divisao,
    safe_cast(descricao_divisao as string) as descricao_divisao,
    safe_cast(secao as string) as secao,
    safe_cast(descricao_secao as string) as descricao_secao
from `basedosdados-staging.br_bd_diretorios_brasil_staging.cnae_2` as t
