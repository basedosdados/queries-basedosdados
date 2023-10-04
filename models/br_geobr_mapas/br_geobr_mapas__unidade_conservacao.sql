{{ 
  config(
    alias='unidade_conservacao',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_unidade_conservacao AS STRING) id_unidade_conservacao,
SAFE_CAST(unidade_conservacao AS STRING) unidade_conservacao,
SAFE_CAST(id_unidade_conservacao_wcmc AS STRING) id_unidade_conservacao_wcmc,
SAFE_CAST(id_cnuc AS STRING) id_cnuc,
SAFE_CAST(id_geografico AS STRING) id_geografico,
SAFE_CAST(organizacao_orgao AS STRING) organizacao_orgao,
SAFE_CAST(categoria AS STRING) categoria,
SAFE_CAST(sigla_grupo AS STRING) sigla_grupo,
SAFE_CAST(qualidade AS STRING) qualidade,
SAFE_CAST(esfera AS STRING) esfera,
SAFE_CAST(ano_criacao AS INT64) ano_criacao,
SAFE_CAST(legislacao AS STRING) legislacao,
SAFE_CAST(data_ultima AS DATE) data_ultima,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.unidade_conservacao as t