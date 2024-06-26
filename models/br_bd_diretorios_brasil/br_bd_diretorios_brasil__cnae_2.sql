{{
    config(
        alias="cnae_2",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(secao as string) as secao,
    safe_cast(descricao_secao as string) as descricao_secao,
    safe_cast(divisao as string) as divisao,
    safe_cast(descricao_divisao as string) as descricao_divisao,
    safe_cast(grupo as string) as grupo,
    safe_cast(descricao_grupo as string) as descricao_grupo,
    safe_cast(classe as string) as classe,
    safe_cast(descricao_classe as string) as descricao_classe,
    safe_cast(subclasse as string) as subclasse,
    safe_cast(descricao_subclasse as string) as descricao_subclasse
from `basedosdados-staging.br_bd_diretorios_brasil_staging.cnae_2` as t
