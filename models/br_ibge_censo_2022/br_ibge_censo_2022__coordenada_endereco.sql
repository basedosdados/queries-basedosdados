{{
config(alias='coordenada_endereco',
schema='br_ibge_censo_2022',
materialized='table',
    partition_by={
      "field": "id_uf",
      "data_type": "int64",
},
cluster_by = ["id_municipio"])}}

SELECT
SAFE_CAST(id_uf AS STRING) id_uf,
SAFE_CAST(COD_MUN AS STRING) id_municipio,
SAFE_CAST(COD_ESPECIE AS STRING) especie_endereco,
SAFE_CAST(NV_GEO_COORD AS STRING) nivel_geo_coordenada,
SAFE_CAST(LATITUDE AS FLOAT64) latitude,
SAFE_CAST(LONGITUDE AS FLOAT64) longitude,
ST_GEOGPOINT(SAFE_CAST(LONGITUDE AS FLOAT64),SAFE_CAST(LATITUDE AS FLOAT64)) ponto
FROM basedosdados-staging.br_ibge_censo_2022_staging.coordenada_endereco AS t



