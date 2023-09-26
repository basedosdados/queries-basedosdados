{{ 
  config(
    alias='setor_censitario_basico_2010',
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
SAFE_CAST(v001 AS FLOAT64) v001,
SAFE_CAST(v002 AS FLOAT64) v002,
SAFE_CAST(v003 AS FLOAT64) v003,
SAFE_CAST(v004 AS FLOAT64) v004,
SAFE_CAST(v005 AS FLOAT64) v005,
SAFE_CAST(v006 AS FLOAT64) v006,
SAFE_CAST(v007 AS FLOAT64) v007,
SAFE_CAST(v008 AS FLOAT64) v008,
SAFE_CAST(v009 AS FLOAT64) v009,
SAFE_CAST(v010 AS FLOAT64) v010,
SAFE_CAST(v011 AS FLOAT64) v011,
SAFE_CAST(v012 AS FLOAT64) v012
from basedosdados-dev.br_ibge_censo_demografico_staging.setor_censitario_basico_2010 as t