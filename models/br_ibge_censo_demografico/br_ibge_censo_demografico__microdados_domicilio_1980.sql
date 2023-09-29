{{ 
  config(
    alias='microdados_domicilio_1980',
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
    SAFE_CAST(id_distrito AS STRING) id_distrito,
    SAFE_CAST(v201 AS STRING) v201,
    SAFE_CAST(v202 AS STRING) v202,
    SAFE_CAST(v203 AS STRING) v203,
    SAFE_CAST(v204 AS STRING) v204,
    SAFE_CAST(v205 AS STRING) v205,
    SAFE_CAST(v206 AS STRING) v206,
    SAFE_CAST(v207 AS STRING) v207,
    SAFE_CAST(v208 AS STRING) v208,
    SAFE_CAST(v209 AS STRING) v209,
    SAFE_CAST(v602 AS INT64) v602,
    SAFE_CAST(v212 AS INT64) v212,
    SAFE_CAST(v213 AS INT64) v213,
    SAFE_CAST(v214 AS STRING) v214,
    SAFE_CAST(v215 AS STRING) v215,
    SAFE_CAST(v216 AS STRING) v216,
    SAFE_CAST(v217 AS STRING) v217,
    SAFE_CAST(v218 AS STRING) v218,
    SAFE_CAST(v219 AS STRING) v219,
    SAFE_CAST(v220 AS STRING) v220,
    SAFE_CAST(v221 AS STRING) v221,
    SAFE_CAST(v198 AS INT64) v198,
    SAFE_CAST(v603 AS INT64) v603,
    SAFE_CAST(v598 AS STRING) v598
from basedosdados-staging.br_ibge_censo_demografico_staging.microdados_domicilio_1980 as t
