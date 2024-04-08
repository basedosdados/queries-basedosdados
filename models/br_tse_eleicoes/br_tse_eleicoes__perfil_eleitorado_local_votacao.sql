{{
    config(
        schema='br_tse_eleicoes',
        alias = 'perfil_eleitorado_local_votacao',
        materialized='table',
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 2016,
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
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_municipio_tse AS STRING) id_municipio_tse,
SAFE_CAST(zona AS STRING) zona,
SAFE_CAST(secao AS STRING) secao,
SAFE_CAST(tipo_secao_agregada AS STRING) tipo_secao_agregada ,
SAFE_CAST(numero AS STRING) numero,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(tipo AS STRING) tipo,
SAFE_CAST(endereco AS STRING) endereco,
SAFE_CAST(bairro AS STRING) bairro,
SAFE_CAST(cep AS STRING) cep,
SAFE_CAST(telefone AS STRING) telefone,
SAFE_CAST(latitude AS FLOAT64) latitude,
SAFE_CAST(longitude AS FLOAT64) longitude,
SAFE_CAST(situacao AS STRING) situacao,
SAFE_CAST(situacao_zona AS STRING) situacao_zona,
SAFE_CAST(situacao_secao AS STRING) situacao_secao,
SAFE_CAST(situacao_localidade AS STRING) situacao_localidade,
SAFE_CAST(situacao_secao_acessibilidade AS STRING) situacao_secao_acessibilidade,
SAFE_CAST(quantidade_eleitores AS INT64) quantidade_eleitores,
SAFE_CAST(quantidade_eleitores_eleicao AS INT64) quantidade_eleitores_eleicao,
FROM basedosdados-staging.br_tse_eleicoes_staging.perfil_eleitorado_local_votacao AS t
















