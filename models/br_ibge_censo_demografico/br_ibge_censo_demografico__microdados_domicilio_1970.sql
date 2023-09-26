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
    SAFE_CAST(v001 AS INT64) v001,
    SAFE_CAST(v002 AS INT64) v002,
    SAFE_CAST(v003 AS INT64) v003,
    SAFE_CAST(v004 AS INT64) v004,
    SAFE_CAST(v005 AS INT64) v005,
    SAFE_CAST(v006 AS INT64) v006,
    SAFE_CAST(v007 AS INT64) v007,
    SAFE_CAST(v008 AS INT64) v008,
    SAFE_CAST(v009 AS INT64) v009,
    SAFE_CAST(v010 AS INT64) v010,
    SAFE_CAST(v011 AS INT64) v011,
    SAFE_CAST(v012 AS INT64) v012,
    SAFE_CAST(v013 AS INT64) v013,
    SAFE_CAST(v014 AS INT64) v014,
    SAFE_CAST(v015 AS INT64) v015,
    SAFE_CAST(v016 AS INT64) v016,
    SAFE_CAST(v017 AS INT64) v017,
    SAFE_CAST(v018 AS INT64) v018,
    SAFE_CAST(v019 AS INT64) v019,
    SAFE_CAST(v020 AS INT64) v020,
    SAFE_CAST(v021 AS INT64) v021,
    SAFE_CAST(v054 AS INT64) v054
FROM basedosdados-dev.br_ibge_censo_demografico_staging.microdados_domicilio_1970 AS t