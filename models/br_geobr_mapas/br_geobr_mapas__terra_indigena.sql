{{ 
  config(
    alias='terra_indigena',
    schema='br_geobr_mapas',
    materialized='table',
    )
 }}
SELECT 
SAFE_CAST(id_geografico AS STRING) id_geografico,
SAFE_CAST(id_terra_indigena AS STRING) id_terra_indigena,
SAFE_CAST(terra_indigena AS STRING) terra_indigena,
SAFE_CAST(etnia AS STRING) etnia,
SAFE_CAST(nome_municipio AS STRING) nome_municipio,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(area AS FLOAT64) area,
SAFE_CAST(fase AS STRING) fase,
SAFE_CAST(modalidade AS STRING) modalidade,
SAFE_CAST(reestudo AS STRING) reestudo,
SAFE_CAST(indicador_faixa_fronteira AS BOOL) indicador_faixa_fronteira,
SAFE_CAST(id_unidade_administrativa AS STRING) id_unidade_administrativa,
SAFE_CAST(sigla_unidade_administrativa AS STRING) sigla_unidade_administrativa,
SAFE_CAST(unidade_administrativa AS STRING) unidade_administrativa,
SAFE.ST_GEOGFROMTEXT(geometria) geometria,
FROM basedosdados-staging.br_geobr_mapas_staging.terra_indigena as t