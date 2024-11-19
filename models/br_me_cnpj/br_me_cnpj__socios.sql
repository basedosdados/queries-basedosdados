{{
    config(
        schema="br_me_cnpj",
        alias="socios",
        materialized="incremental",
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
    cnpj_socios as (
        select
            safe_cast(data as date) data,
            safe_cast(lpad(cnpj_basico, 8, '0') as string) cnpj_basico,
            safe_cast(tipo as string) tipo,
            safe_cast(nome as string) nome,
            safe_cast(documento as string) documento,
            safe_cast(cast(qualificacao as int64) as string) qualificacao,
            safe_cast(data_entrada_sociedade as date) data_entrada_sociedade,
            safe_cast(cast(id_pais as int64) as string) id_pais,
            safe_cast(cpf_representante_legal as string) cpf_representante_legal,
            safe_cast(nome_representante_legal as string) nome_representante_legal,
            safe_cast(
                cast(qualificacao_representante_legal as int64) as string
            ) qualificacao_representante_legal,
            safe_cast(faixa_etaria as string) faixa_etaria
        from `basedosdados-staging.br_me_cnpj_staging.socios` as t
        where qualificacao != "qualificacao"
    )
select *
from cnpj_socios
{% if is_incremental() %} where data > '2024-11-15' {% endif %}
