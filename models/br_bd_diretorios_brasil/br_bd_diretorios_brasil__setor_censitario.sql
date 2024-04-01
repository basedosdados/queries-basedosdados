{{
    config(
        alias="setor_censitario",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
with
    censo_2022 as (
        select
            2022 as ano,
            cd_setor as id_setor_censitario,
            cd_mun as id_municipio,
            cast(null as string) as id_rm,
            cast(null as string) nome_rm,
            cd_dist as id_distrito,
            nm_dist as nome_distrito,
            cd_subdist as id_subdistrito,
            nm_subdist as nome_subdistrito,
            cast(null as string) id_bairro,
            cast(null as string) nome_bairro,
            cast(null as string) sigla_uf,
            cast(null as string) situacao_setor,
            cast(null as string) tipo_setor,
            cd_micro as id_microrregiao,
            nm_micro as nome_microrregiao,
            cd_meso as id_mesorregiao,
            nm_meso as nome_mesorregiao,
            cd_rgi as id_regiao_imediata,
            nm_rgi as nome_regiao_imediata,
            cd_rgint as id_regiao_intermediaria,
            nm_rgint as nome_regiao_intermediaria,
            cd_concurb as id_concentracao_urbana,
            nm_concurb as nome_concentracao_urbana,
        from
            `basedosdados-staging.br_ibge_censo_2022_staging.domicilio_morador_setor_censitario`

    ),

    censo_2010 as (
        select
            *,
            cast(null as string) id_microrregiao,
            cast(null as string) nome_microrregiao,
            cast(null as string) id_mesorregiao,
            cast(null as string) nome_mesorregiao,
            cast(null as string) id_regiao_imediata,
            cast(null as string) nome_regiao_imediata,
            cast(null as string) id_regiao_intermediaria,
            cast(null as string) nome_regiao_intermediaria,
            cast(null as string) id_concentracao_urbana,
            cast(null as string) nome_concentracao_urbana
        from `basedosdados-staging.br_bd_diretorios_brasil.setor_censitario`
    )

select *
from censo_2022
union all
select *
from censo_2010
