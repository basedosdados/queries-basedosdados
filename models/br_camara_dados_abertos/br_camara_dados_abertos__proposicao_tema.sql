{{
    config(
        alias="proposicao_tema",
        schema="br_camara_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1935, "end": 2024, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(uriproposicao as string) url_proposicao,
    safe_cast(siglatipo as string) tipo_proposicao,
    safe_cast(numero as string) numero,
    safe_cast(codtema as string) tema,
    safe_cast(relevancia as int64) relevancia,
from `basedosdados-staging.br_camara_dados_abertos_staging.proposicao_tema ` as t
