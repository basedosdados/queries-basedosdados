{{ config(
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
    cluster_by = ["mes","sigla_uf_ncm"],
    labels = {'project_id': 'basedosdados', 'tema': 'economia'},
    post_hook=['REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
                'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"'])
 }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_ncm AS STRING) id_ncm,
SAFE_CAST(id_unidade AS STRING) id_unidade,
SAFE_CAST(id_pais AS STRING) id_pais,
SAFE_CAST(sigla_uf_ncm AS STRING) sigla_uf_ncm,
SAFE_CAST(id_via AS STRING) id_via,
SAFE_CAST(id_urf AS STRING) id_urf,
SAFE_CAST(quantidade_estatistica AS INT64) quantidade_estatistica,
SAFE_CAST(peso_liquido_kg AS INT64) peso_liquido_kg,
SAFE_CAST(valor_fob_dolar AS INT64) valor_fob_dolar
FROM basedosdados-staging.br_me_comex_stat_staging.ncm_exportacao AS t