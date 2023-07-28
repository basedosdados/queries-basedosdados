{{ config(
    alias='microdados_atualizado',
    schema='br_anatel_telefonia_movel',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2019,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["id_municipio", "mes"],
    labels = {'project_id': 'basedosdados'},
    post_hook=['REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
              'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"'])
 }}


SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(ddd AS STRING) ddd,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(cnpj AS STRING) cnpj,
SAFE_CAST(empresa AS STRING) empresa,
SAFE_CAST(porte_empresa AS STRING) porte_empresa,
SAFE_CAST(tecnologia AS STRING) tecnologia,
SAFE_CAST(sinal AS STRING) sinal,
SAFE_CAST(modalidade AS STRING) modalidade,
SAFE_CAST(pessoa AS STRING) pessoa,
SAFE_CAST(produto AS STRING) produto,
SAFE_CAST(acessos AS INT64) acessos

FROM basedosdados-dev.br_anatel_telefonia_movel_staging.microdados AS t
