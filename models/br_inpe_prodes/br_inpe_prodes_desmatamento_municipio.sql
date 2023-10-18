{{
    config(
        alias='desmatamento_municipio',
        schema='br_inpe_prodes'
    )
}}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(bioma AS STRING) bioma,
SAFE_CAST(area AS INT64) area,
SAFE_CAST(desmatamento AS FLOAT64) desmatamento,
SAFE_CAST(floresta AS FLOAT64) floresta,
SAFE_CAST(nao_floresta AS FLOAT64) nao_floresta,
SAFE_CAST(hidrografia AS FLOAT64) hidrografia
FROM basedosdados-staging.br_inpe_prodes_staging.desmatamento_municipio AS t
