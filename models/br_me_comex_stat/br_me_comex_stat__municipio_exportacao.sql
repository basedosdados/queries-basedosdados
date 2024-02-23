{{
    config(
        alias="municipio_exportacao",
        schema="br_me_comex_stat",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1997, "end": 2023, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
        labels={"project_id": "basedosdados", "tema": "economia"},
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6 OR DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 0)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) < 6 OR DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) < 0)',
        ],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_sh4 as string) id_sh4,
    safe_cast(id_pais as string) id_pais,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(peso_liquido_kg as int64) peso_liquido_kg,
    safe_cast(valor_fob_dolar as int64) valor_fob_dolar
from `basedosdados-staging.br_me_comex_stat_staging.municipio_exportacao` as t
