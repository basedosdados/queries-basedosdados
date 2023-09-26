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
    SAFE_CAST(v201 AS INT64) v201,
    SAFE_CAST(v202 AS INT64) v202,
    SAFE_CAST(v203 AS INT64) v203,
    SAFE_CAST(v204 AS INT64) v204,
    SAFE_CAST(v205 AS INT64) v205,
    SAFE_CAST(v206 AS INT64) v206,
    SAFE_CAST(v207 AS INT64) v207,
    SAFE_CAST(v208 AS INT64) v208,
    SAFE_CAST(v209 AS INT64) v209,
    SAFE_CAST(v602 AS INT64) v602,
    SAFE_CAST(v212 AS INT64) v212,
    SAFE_CAST(v213 AS INT64) v213,
    SAFE_CAST(v214 AS INT64) v214,
    SAFE_CAST(v215 AS INT64) v215,
    SAFE_CAST(v216 AS INT64) v216,
    SAFE_CAST(v217 AS INT64) v217,
    SAFE_CAST(v218 AS INT64) v218,
    SAFE_CAST(v219 AS INT64) v219,
    SAFE_CAST(v220 AS INT64) v220,
    SAFE_CAST(v221 AS INT64) v221,
    SAFE_CAST(v198 AS INT64) v198,
    SAFE_CAST(v603 AS INT64) v603,
    SAFE_CAST(v598 AS INT64) v598
from basedosdados-dev.br_ibge_censo_demografico_staging.microdados_domicilio_1980 as t
