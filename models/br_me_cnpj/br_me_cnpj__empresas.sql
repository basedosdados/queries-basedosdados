{{
    config(
        schema="br_me_cnpj",
        materialized="incremental",
        alias="empresas",
        unique_key="data",
        partition_by={
            "field": "data",
            "data_type": "date",
        },
        incremental_strategy="insert_overwrite",
        pre_hook=[
            "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
            "DELETE FROM {{ this }}         WHERE data > '2024-11-15'",
        ],
    )
}}
with
    cnpj_empresas as (
        select
            safe_cast(data as date) data,
            safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
            safe_cast(razao_social as string) razao_social,
            safe_cast(natureza_juridica as string) natureza_juridica,
            safe_cast(qualificacao_responsavel as string) qualificacao_responsavel,
            safe_cast(capital_social as float64) capital_social,
            safe_cast(regexp_replace(porte, '^0', '') as string) porte,
            safe_cast(ente_federativo as string) ente_federativo
        from `basedosdados-staging.br_me_cnpj_staging.empresas` as t
        where porte != "porte"
    )
select *
from cnpj_empresas
{% if is_incremental() %} where data = '2024-11-15' {% endif %}
