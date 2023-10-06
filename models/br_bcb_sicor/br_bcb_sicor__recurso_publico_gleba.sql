{{
  config(
    alias = 'recurso_publico_gleba',
    schema='br_bcb_sicor',
    materialized='table'
  )
}}
SELECT
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(numero_ordem AS STRING) numero_ordem,
SAFE_CAST(numero_identificador_gleba AS STRING) numero_identificador_gleba,
SAFE_CAST(indice_indice_gleba AS INT64) indice_gleba,
SAFE_CAST(indice_indice_ponto AS INT64) indice_ponto,
ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64),SAFE_CAST(latitude AS FLOAT64)) ponto,
SAFE_CAST(altitude AS FLOAT64) altitude
FROM basedosdados-staging.br_bcb_sicor_staging.recurso_publico_gleba AS t