{{ 
  config(
    alias='concentracao_urbana',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_concentracao_urbana AS STRING) id_concentracao_urbana,
SAFE_CAST(concentracao_urbana AS STRING) concentracao_urbana,
SAFE_CAST(populacao_urbana_2010 AS INT64) populacao_urbana_2010,
SAFE_CAST(populacao_rural_2010 AS INT64) populacao_rural_2010,
SAFE_CAST(populacao_2010 AS INT64) populacao_2010,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.concentracao_urbana as t