{{
    config(
        schema='br_tse_eleicoes',
        alias = 'perfil_eleitorado_municipio_zona',
        materialized='table',
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {
                "start": 1998,
                "end": 2022,
                "interval": 2
            }
        },
        cluster_by=["sigla_uf"],
    )
}}

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_municipio_tse AS STRING) id_municipio_tse,
SAFE_CAST(situacao_biometria AS STRING) situacao_biometria,
SAFE_CAST(zona AS STRING) zona,
SAFE_CAST(genero AS STRING) genero,
SAFE_CAST(estado_civil AS STRING) estado_civil,
SAFE_CAST(grupo_idade AS STRING) grupo_idade,
SAFE_CAST(instrucao AS STRING) instrucao,
SAFE_CAST(eleitores AS STRING) eleitores,
SAFE_CAST(eleitores_biometria AS STRING) eleitores_biometria,
SAFE_CAST(eleitores_deficiencia AS STRING) eleitores_deficiencia
FROM basedosdados-staging.br_tse_eleicoes_staging.perfil_eleitorado_municipio_zona AS t
















