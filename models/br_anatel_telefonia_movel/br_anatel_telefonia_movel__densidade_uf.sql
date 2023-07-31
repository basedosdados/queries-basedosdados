{{ config(alias='densidade_uf', schema='br_anatel_telefonia_movel') }}
 
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(densidade AS FLOAT64) densidade

FROM basedosdados-staging.br_anatel_telefonia_movel_staging.densidade_uf AS t 
WHERE (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6