{{
    config(
        alias="votacao_orientacao_bancada",
        schema="br_camara_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2023, "interval": 1},
        },
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_votacao as string) id_votacao,
    safe_cast(sigla_orgao as string) sigla_orgao,
    safe_cast(descricao as string) descricao,
    safe_cast(id_proposicao as string) sigla_bancada,
    safe_cast(orientacao as string) orientacao,
from `basedosdados-staging.br_camara_dados_abertos_staging.votacao_orientacao_bancada` t
