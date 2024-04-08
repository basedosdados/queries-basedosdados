{{
    config(
        schema='br_tse_eleicoes',
        alias = 'partidos',
        materialized='table',
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 1990,
                "end": 2022,
                "interval": 2
            }
        },
        cluster_by=["sigla_uf"],
    )
}}

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(turno AS INT64) turno,
SAFE_CAST(tipo_eleicao AS STRING) tipo_eleicao,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_municipio_tse AS STRING) id_municipio_tse,
SAFE_CAST(cargo AS STRING) cargo,
SAFE_CAST(numero AS STRING) numero,
SAFE_CAST(sigla AS STRING) sigla,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(tipo_agremiacao AS STRING) tipo_agremiacao,
SAFE_CAST(sequencial_coligacao AS STRING) sequencial_coligacao,
SAFE_CAST(nome_coligacao AS STRING) nome_coligacao,
SAFE_CAST(composicao_coligacao AS STRING) composicao_coligacao,
SAFE_CAST(numero_federacao AS STRING) numero_federacao,
SAFE_CAST(nome_federacacao AS STRING) nome_federacacao,
SAFE_CAST(sigla_federacao AS STRING) sigla_federacao,
SAFE_CAST(composicao_federacao AS STRING) composicao_federacao,
SAFE_CAST(situacao_legenda AS STRING) situacao_legenda
FROM basedosdados-staging.br_tse_eleicoes_staging.partidos AS t

















