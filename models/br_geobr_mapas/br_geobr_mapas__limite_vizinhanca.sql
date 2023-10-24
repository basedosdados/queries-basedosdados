{{ 
  config(
    alias='limite_vizinhanca',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(REPLACE(id_uf,".0","") AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(REPLACE(id_municipio,".0","") AS STRING) id_municipio,
SAFE_CAST(nome_municipio AS STRING) nome_municipio,
SAFE_CAST(REPLACE(id_distrito,".0","") AS STRING) id_distrito,
SAFE_CAST(nome_distrito AS STRING) nome_distrito,
SAFE_CAST(REPLACE(id_subdistrito,".0","") AS STRING) id_subdistrito,
SAFE_CAST(nome_subdistrito AS STRING) nome_subdistrito,
SAFE_CAST(REPLACE(id_vizinhanca,".0","") AS STRING) id_vizinhanca,
SAFE_CAST(nome_vizinhanca AS STRING) nome_vizinhanca,
SAFE_CAST(referencia_geometria AS STRING) referencia_geometria,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.limite_vizinhanca as t