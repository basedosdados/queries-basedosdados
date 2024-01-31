{{ config(alias='municipio',schema='br_ibge_pib') }}
SELECT 
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(pib AS INT64) pib,
SAFE_CAST(impostos_liquidos AS INT64) impostos_liquidos,
SAFE_CAST(va AS INT64) va,
SAFE_CAST(va_agropecuaria AS INT64) va_agropecuaria,
SAFE_CAST(va_industria AS INT64) va_industria,
SAFE_CAST(va_servicos AS INT64) va_servicos,
SAFE_CAST(va_adespss AS INT64) va_adespss
FROM basedosdados-staging.br_ibge_pib_staging.municipio AS t
