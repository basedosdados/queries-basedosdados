{{
    config(
        schema="br_rf_arrecadacao",
        alias="itr",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2017, "end": 2024, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sigla_regiao as string) sigla_regiao,
    safe_cast(cidade as string) cidade,
    safe_cast(valor_arrecadado as float64) valor_arrecadado,
from `basedosdados-staging.br_rf_arrecadacao_staging.itr` as t
