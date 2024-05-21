{{ config(alias="uf", schema="br_inep_saeb", materialized="table") }}

with
    tb as (
        select
            safe_cast(ano as int64) ano,
            safe_cast(rede as string) rede,
            safe_cast(localizacao as string) localizacao,
            safe_cast(sigla_uf as string) sigla_uf,
            safe_cast(disciplina as string) disciplina,
            safe_cast(serie as int64) serie,
            safe_cast(media as float64) media,
            safe_cast(nivel_0 as float64) nivel_0,
            safe_cast(nivel_1 as float64) nivel_1,
            safe_cast(nivel_2 as float64) nivel_2,
            safe_cast(nivel_3 as float64) nivel_3,
            safe_cast(nivel_4 as float64) nivel_4,
            safe_cast(nivel_5 as float64) nivel_5,
            safe_cast(nivel_6 as float64) nivel_6,
            safe_cast(nivel_7 as float64) nivel_7,
            safe_cast(nivel_8 as float64) nivel_8,
            safe_cast(nivel_9 as float64) nivel_9,
            safe_cast(nivel_10 as float64) nivel_10,
        from `basedosdados-staging.br_inep_saeb_staging.uf` as t
    ),
    fixed_2021 as (
        select *
        from tb
        -- Em 2021 as linhas estao duplicadas porque tem `Total` e `total`
        -- Entao vamos excluir total
        where ano = 2021 and localizacao in ("Total", "Urbana", "Rural")
    ),
    rest_without_2021 as (select * from tb where ano <> 2021)

select
    ano,
    lower(rede) as rede,
    lower(localizacao) as localizacao,
    sigla_uf,
    disciplina,
    serie,
    media,
    nivel_0,
    nivel_1,
    nivel_2,
    nivel_3,
    nivel_4,
    nivel_5,
    nivel_6,
    nivel_7,
    nivel_8,
    nivel_9,
    nivel_10
from
    (
        select *
        from rest_without_2021
        union all
        select *
        from fixed_2021
    )
order by ano desc
