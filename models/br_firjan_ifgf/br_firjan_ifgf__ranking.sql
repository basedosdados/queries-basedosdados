{{ 
    config(
        alias='ranking', 
        schema='br_firjan_ifgf',
    labels = {'tema': 'economia'}
    )
 }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(indice_firjan_gestao_fiscal AS FLOAT64) indice_firjan_gestao_fiscal,
SAFE_CAST(ranking_estadual AS INT64) ranking_estadual,
SAFE_CAST(ranking_nacional AS INT64) ranking_nacional,
FROM basedosdados-staging.br_firjan_ifgf_staging.ranking AS t

