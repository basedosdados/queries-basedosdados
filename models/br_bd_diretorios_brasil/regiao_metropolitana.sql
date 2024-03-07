{{
    config(
        schema="br_bd_diretorios_brasil",
        materialized="table",
    )
}}
select
    safe_cast(id_regiao_metropolitana as string) id_regiao_metropolitana,
    safe_cast(nome as string) nome,
    safe_cast(id_recorte_metropolitano as string) id_recorte_metropolitano,
    safe_cast(nome_recorte_metropolitano as string) nome_recorte_metropolitano,
    safe_cast(id_subcategoria_metropolitana as string) id_subcategoria_metropolitana,
    safe_cast(
        nome_subcategoria_metropolitana as string
    ) nome_subcategoria_metropolitana,
    safe_cast(tipo as string) tipo,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(nome_regiao as string) nome_regiao,
from `basedosdados-staging.br_bd_diretorios_brasil_staging.regiao_metropolitana` as t
