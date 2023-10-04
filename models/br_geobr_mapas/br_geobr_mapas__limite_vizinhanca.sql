{{ 
  config(
    alias='limite_vizinhanca',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_uf AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(nome_municipio AS STRING) nome_municipio,
SAFE_CAST(id_distrito AS STRING) id_distrito,
SAFE_CAST(nome_distrito AS STRING) nome_distrito,
SAFE_CAST(id_subdistrito AS STRING) id_subdistrito,
SAFE_CAST(nome_subdistrito AS STRING) nome_subdistrito,
SAFE_CAST(id_vizinhanca AS STRING) id_vizinhanca,
SAFE_CAST(nome_vizinhanca AS STRING) nome_vizinhanca,
SAFE_CAST(referencia_geometria AS STRING) referencia_geometria,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.limite_vizinhanca as t