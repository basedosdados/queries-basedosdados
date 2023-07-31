{{ config(
    alias='microdados',
    schema='br_anatel_banda_larga_fixa',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2007,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["id_municipio", "mes"],
    labels = {'project_id': 'basedosdados'})
 }}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(cnpj AS STRING) cnpj,
SAFE_CAST(empresa AS STRING) empresa,
SAFE_CAST(porte_empresa AS STRING) porte_empresa,
SAFE_CAST(tecnologia AS STRING) tecnologia,
SAFE_CAST(transmissao AS STRING) transmissao,
SAFE_CAST(velocidade AS STRING) velocidade,
SAFE_CAST(produto AS STRING) produto,
SAFE_CAST(acessos AS INT64) acessos
FROM basedosdados-staging.br_anatel_banda_larga_fixa_staging.microdados AS t
WHERE (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6
  OR  DATE_DIFF(DATE(2023,5,1),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 0)