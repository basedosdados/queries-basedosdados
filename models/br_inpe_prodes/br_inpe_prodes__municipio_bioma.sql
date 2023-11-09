{{
    config(
        alias='municipio_bioma',
        schema='br_inpe_prodes'
    )
}}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(bioma AS STRING) bioma,
SAFE_CAST(area AS INT64) area_total,
SAFE_CAST(desmatamento AS FLOAT64) desmatado,
SAFE_CAST(floresta AS FLOAT64) vegetacao_natural,
SAFE_CAST(nao_floresta AS FLOAT64) nao_vegetacao_natural,
SAFE_CAST(hidrografia AS FLOAT64) hidrografia
FROM basedosdados-staging.br_inpe_prodes_staging.municipio_bioma AS t
