{{
    config(
        alias="eleicao_auxiliar_categoria_origem",
        schema="br_jota",
        materialized="table",
    )
}}
select *
from `basedosdados-staging.br_jota_staging.eleicao_auxiliar_categoria_origem`
