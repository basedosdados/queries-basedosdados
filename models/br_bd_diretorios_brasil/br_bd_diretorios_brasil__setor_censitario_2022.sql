{{
    config(
        alias="setor_censitario_2022",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(cd_setor as string) as id_setor_censitario,
    safe_cast(area_km2 as float64) as area_km2,
    safe_cast(cd_regiao as string) as id_regiao,
    safe_cast(nm_regiao as string) as nome_regiao,
    safe_cast(cd_uf as string) as id_uf,
    safe_cast(nm_uf as string) as nome_uf,
    safe_cast(cd_mun as string) as id_municipio,
    safe_cast(nm_mun as string) as nome_municipio,
    safe_cast(cd_dist as string) as id_distrito,
    safe_cast(nm_dist as string) as nome_distrito,
    safe_cast(cd_subdist as string) as id_subdistrito,
    safe_cast(nm_subdist as string) as nome_subdistrito,
    safe_cast(cd_micro as string) as id_microrregiao,
    safe_cast(nm_micro as string) as nome_microrregiao,
    safe_cast(cd_meso as string) as id_mesorregiao,
    safe_cast(nm_meso as string) as nome_mesorregiao,
    safe_cast(cd_rgi as string) as id_regiao_imediata,
    safe_cast(nm_rgi as string) as nome_regiao_imediata,
    safe_cast(cd_rgint as string) as id_regiao_intermediaria,
    safe_cast(nm_rgint as string) as nome_regiao_intermediaria,
    safe_cast(cd_concurb as string) as id_concentracao_urbana,
    safe_cast(nm_concurb as string) as nome_concentracao_urbana,
from `basedosdados-dev.br_ibge_censo_2022_staging.domicilio_morador_setor_censitario`
