{{ config(alias='municipio', schema='br_bcb_estban') }}

SELECT 
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(mes AS INT64) mes,
    SAFE_CAST(sigla_uf AS STRING) sigla_uf,
    SAFE_CAST(id_municipio AS STRING) id_municipio,
    SAFE_CAST(cnpj_basico AS STRING) cnpj_basico,
    SAFE_CAST(instituicao AS STRING) instituicao,
    SAFE_CAST(agencias_esperadas AS INT64) agencias_esperadas,
    SAFE_CAST(agencias_processadas AS INT64) agencias_processadas,
    SAFE_CAST(id_verbete AS STRING) id_verbete,
    SAFE_CAST(valor AS FLOAT64) valor
FROM basedosdados-dev.br_bcb_estban_staging.municipio AS t