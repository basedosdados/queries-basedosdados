{{ config(alias='densidade_uf', schema='br_anatel_banda_larga_fixa') }}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(densidade AS STRING) densidade,

FROM basedosdados-dev.br_anatel_banda_larga_fixa_staging.densidade_uf AS t