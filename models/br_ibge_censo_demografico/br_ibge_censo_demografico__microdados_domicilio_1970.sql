{{ 
  config(
    alias='microdados_domicilio_1970',
    schema='br_ibge_censo_demografico',
    materialized='table',
    partition_by={
      "field": "sigla_uf",
      "data_type": "string",
    },
    )
 }}

SELECT 
    SAFE_CAST(sigla_uf AS STRING) sigla_uf,
    SAFE_CAST(id_municipio AS STRING) id_municipio,
    SAFE_CAST(id_domicilio AS STRING) id_domicilio,
    SAFE_CAST(numero_familia AS INT64) numero_familia,
    SAFE_CAST(v001 AS STRING) v001,
    SAFE_CAST(v002 AS STRING) v002,
    SAFE_CAST(v003 AS STRING) v003,
    SAFE_CAST(v004 AS STRING) v004,
    SAFE_CAST(v005 AS INT64) v005,
    SAFE_CAST(v006 AS STRING) v006,
    SAFE_CAST(v007 AS STRING) v007,
    SAFE_CAST(v008 AS STRING) v008,
    SAFE_CAST(v009 AS STRING) v009,
    SAFE_CAST(v010 AS STRING) v010,
    SAFE_CAST(v011 AS STRING) v011,
    SAFE_CAST(v012 AS STRING) v012,
    SAFE_CAST(v013 AS STRING) v013,
    SAFE_CAST(v014 AS STRING) v014,
    SAFE_CAST(v015 AS STRING) v015,
    SAFE_CAST(v016 AS STRING) v016,
    SAFE_CAST(v017 AS STRING) v017,
    SAFE_CAST(v018 AS STRING) v018,
    SAFE_CAST(v019 AS STRING) v019,
    SAFE_CAST(v020 AS INT64) v020,
    SAFE_CAST(v021 AS INT64) v021,
    SAFE_CAST(v054 AS INT64) v054
FROM basedosdados-staging.br_ibge_censo_demografico_staging.microdados_domicilio_1970 AS t