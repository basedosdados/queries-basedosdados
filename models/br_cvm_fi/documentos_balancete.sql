{{ 
  config(
    schema='br_cvm_fi',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2005,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["mes", "data_competencia"],
    labels = {'project_id': 'basedosdados', 'tema': 'economia'})
 }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
<<<<<<< HEAD
SAFE_CAST(cnpj AS STRING) cnpj,
=======
REGEXP_REPLACE(cnpj, r'[^0-9]', '') AS cnpj,
SUBSTR(REGEXP_REPLACE(cnpj, r'[^0-9]', ''), 1, 8) AS cnpj_basico,
>>>>>>> a58bec35d868cf33544879135c2ac4fcb2c8e480
SAFE_CAST(data_competencia AS DATE) data_competencia,
SAFE_CAST(plano_contabil_balancete AS STRING) plano_contabil_balancete,
SAFE_CAST(codigo_conta AS STRING) codigo_conta,
SAFE_CAST(saldo_conta AS FLOAT64) saldo_conta,
FROM basedosdados-staging.br_cvm_fi_staging.documentos_balancete AS t


