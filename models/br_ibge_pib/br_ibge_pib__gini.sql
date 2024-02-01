{{ config(alias='gini',schema='br_ibge_pib') }}
SELECT
SAFE_CAST(cod AS STRING) id_uf,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(REPLACE(gini_pib, ",", ".") AS FLOAT64) gini_pib,
SAFE_CAST(REPLACE(gini_va_agro, ",", ".") AS FLOAT64) gini_va_agro,
SAFE_CAST(REPLACE(gini_va_industria, ",", ".") AS FLOAT64) gini_va_industria,
SAFE_CAST(REPLACE(gini_servicos, ",", ".")AS FLOAT64) gini_va_servicos,
SAFE_CAST(REPLACE(gini_va_adespss, ",", ".") AS FLOAT64) gini_va_adespss,
FROM basedosdados-staging.br_ibge_pib_staging.gini AS t

