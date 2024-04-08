{{
    config(
        schema='br_tse_eleicoes',
        alias = 'vagas',
        materialized='table',
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 1994,
                "end": 2022,
                "interval": 2
            }
        },
        cluster_by=["sigla_uf"],
    )
}}

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(tipo_eleicao AS STRING) tipo_eleicao,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_municipio_tse AS STRING) id_municipio_tse,
SAFE_CAST(cargo AS STRING) cargo,
SAFE_CAST(vagas AS INT64) vagas
FROM basedosdados-staging.br_tse_eleicoes_staging.vagas AS t