{{ config(alias='densidade_brasil', schema='br_anatel_telefonia_movel') }}
 
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(densidade AS FLOAT64) densidade

FROM basedosdados-staging.br_anatel_telefonia_movel_staging.densidade_brasil AS t