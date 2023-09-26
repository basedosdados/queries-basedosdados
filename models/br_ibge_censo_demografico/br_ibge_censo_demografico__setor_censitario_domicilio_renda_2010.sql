{{ 
  config(
    alias='setor_censitario_domicilio_renda_2010',
    schema='br_ibge_censo_demografico',
    materialized='table',
    partition_by={
      "field": "sigla_uf",
      "data_type": "string",
    },
    )
 }}
SELECT 
    SAFE_CAST(id_setor_censitario AS STRING) id_setor_censitario,
    SAFE_CAST(sigla_uf AS STRING) sigla_uf,
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
    SAFE_CAST(v014 AS INT64) v014
from basedosdados-dev.br_ibge_censo_demografico_staging.setor_censitario_domicilio_renda_2010 as t