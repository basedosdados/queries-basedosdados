{{
    config(
        schema="br_cvm_fi",
        alias="documentos_balancete",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2025, "interval": 1},
        },
        cluster_by=["mes", "data_competencia"],
        labels={"project_id": "basedosdados", "tema": "economia"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(cnpj as string) cnpj,
    safe_cast(data_competencia as date) data_competencia,
    safe_cast(tipo_fundo as string) tipo_fundo,
    safe_cast(plano_contabil_balancete as string) plano_contabil_balancete,
    safe_cast(codigo_conta as string) codigo_conta,
    safe_cast(saldo_conta as float64) saldo_conta,
from `basedosdados-staging.br_cvm_fi_staging.documentos_balancete` as t
