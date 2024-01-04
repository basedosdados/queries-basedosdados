{{
    config(
        alias='violencia_escola',
        schema='br_fbsp_absp'
    )
}}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(uf AS STRING) uf,
SAFE_CAST(tema AS STRING) tema,
SAFE_CAST(item AS STRING) item,
SAFE_CAST(quantidade_escola AS FLOAT64) quantidade_escola
FROM basedosdados-staging.br_fbsp_absp_staging.violencia_escola AS t