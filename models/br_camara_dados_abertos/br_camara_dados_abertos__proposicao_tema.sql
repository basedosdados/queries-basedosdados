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
    safe_cast(replace(ano, ".0", "") as int64) ano,
    regexp_extract(uriproposicao, r'/proposicoes/(\d+)') as id_proposicao,
    safe_cast(siglatipo as string) tipo_proposicao,
    safe_cast(numero as string) numero,
    safe_cast(codtema as string) tema,
    safe_cast(relevancia as int64) relevancia,
from `basedosdados-staging.br_camara_dados_abertos_staging.proposicao_tema` as t
