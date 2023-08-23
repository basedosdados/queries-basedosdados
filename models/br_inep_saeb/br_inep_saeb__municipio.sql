{{ config(
    alias='municipio',
    schema='br_inep_saeb',
    materialized='table',
    labels = {'tema': 'educacao'})
 }}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(rede AS STRING) rede,
SAFE_CAST(localizacao AS STRING) localizacao,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(disciplina AS STRING) disciplina,
SAFE_CAST(serie AS INT64) serie,
ROUND(SAFE_CAST(media AS FLOAT64),2) media,
ROUND(SAFE_CAST(nivel_0 AS FLOAT64),2) nivel_0,
ROUND(SAFE_CAST(nivel_1 AS FLOAT64),2) nivel_1,
ROUND(SAFE_CAST(nivel_2 AS FLOAT64),2) nivel_2,
ROUND(SAFE_CAST(nivel_3 AS FLOAT64),2) nivel_3,
ROUND(SAFE_CAST(nivel_4 AS FLOAT64),2) nivel_4,
ROUND(SAFE_CAST(nivel_5 AS FLOAT64),2) nivel_5,
ROUND(SAFE_CAST(nivel_6 AS FLOAT64),2) nivel_6,
ROUND(SAFE_CAST(nivel_7 AS FLOAT64),2) nivel_7,
ROUND(SAFE_CAST(nivel_8 AS FLOAT64),2) nivel_8,
ROUND(SAFE_CAST(nivel_9 AS FLOAT64),2) nivel_9,
ROUND(SAFE_CAST(nivel_10 AS FLOAT64),2) nivel_10
FROM basedosdados-staging.br_inep_saeb_staging.municipio AS t