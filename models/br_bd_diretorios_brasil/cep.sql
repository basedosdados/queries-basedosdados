{{
  config(
    schema='br_bd_diretorios_brasil',
    materialized='table',
    partition_by={
      "field": "sigla_uf",
      "data_type": "string",
    }
  )
}}

SELECT 
SAFE_CAST(lpad(cep, 8, '0') AS STRING) cep,
SAFE_CAST(logradouro AS STRING) logradouro,
SAFE_CAST(complemento AS STRING) complemento,
SAFE_CAST(bairro AS STRING) bairro,
SAFE_CAST(cidade AS STRING) cidade,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(latitude AS FLOAT64) latitude,
SAFE_CAST(longitude AS FLOAT64) longitude,
ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64),SAFE_CAST(latitude AS FLOAT64)) centroide
FROM basedosdados-staging.br_bd_diretorios_brasil_staging.cep AS t