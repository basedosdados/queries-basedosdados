{{
    config(
        alias="cnaes",
        schema="br_rf_cno",
        materialized="incremental",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
        },
    )
}}
select
    safe_cast(data as date) data_extracao,
    safe_cast(data_registro as date) data_registro,
    safe_cast(id_cno as string) id_cno,
    safe_cast(cnae_2_subclasse as string) cnae_2_subclasse,
from `basedosdados-staging.br_rf_cno_staging.cnaes` as t
{% if is_incremental() %} where data > (select max(data) from {{ this }}) {% endif %}
