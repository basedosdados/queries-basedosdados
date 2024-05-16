{{
    config(
        alias="setor_censitario_2010",
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}

select
    safe_cast(id_setor_censitario as string) id_setor_censitario,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_rm as string) id_rm,
    safe_cast(nome_rm as string) nome_rm,
    safe_cast(id_distrito as string) id_distrito,
    safe_cast(nome_distrito as string) nome_distrito,
    safe_cast(id_subdistrito as string) id_subdistrito,
    safe_cast(nome_subdistrito as string) nome_subdistrito,
    safe_cast(id_bairro as string) id_bairro,
    safe_cast(nome_bairro as string) nome_bairro,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(situacao_setor as string) situacao_setor,
    safe_cast(tipo_setor as string) tipo_setor
from `basedosdados-staging.br_bd_diretorios_brasil_staging.setor_censitario` as t
