{{ 
  config(
    schema='br_me_comex_stat',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1997,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["mes","sigla_uf"],
    labels = {'project_id': 'basedosdados', 'tema': 'economia'})
 }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_sh4 AS STRING) id_sh4,
SAFE_CAST(id_pais AS STRING) id_pais,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(peso_liquido_kg AS INT64) peso_liquido_kg,
SAFE_CAST(valor_fob_dolar AS INT64) valor_fob_dolar
FROM basedosdados-staging.br_me_comex_stat_staging.municipio_importacao AS t
WHERE DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6