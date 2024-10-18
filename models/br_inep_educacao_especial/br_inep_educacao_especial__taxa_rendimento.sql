{{
    config(
        alias="taxa_rendimento",
        schema="br_inep_educacao_especial",
        materialized="table",
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(etapa_ensino as string) etapa_ensino,
    safe_cast(taxa_aprovacao as numeric) taxa_aprovacao,
    safe_cast(taxa_reprovacao as numeric) taxa_reprovacao,
    safe_cast(taxa_abandono as numeric) taxa_abandono,
from `basedosdados-staging.br_inep_educacao_especial_staging.taxa_rendimento` as t
