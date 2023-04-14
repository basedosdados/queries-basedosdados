{{ config(alias='densidade_brasil', schema='br_anatel_banda_larga_fixa') }}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(densidade AS STRING) densidade,

FROM basedosdados-dev.br_anatel_banda_larga_fixa_staging.densidade_brasil AS t