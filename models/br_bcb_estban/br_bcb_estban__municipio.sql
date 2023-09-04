{{ 
  config(
    alias = 'municipio',
    schema='br_bcb_estban',
    materialized='incremental',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1987,
        "end": 2023,
        "interval": 1}
    },
    cluster_by = ["mes", "sigla_uf"],
    labels = {'project_id': 'basedosdados-dev', 'tema': 'economia'},
    pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    post_hook = [ 
      'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
      'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
       ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)'      
     ])
 }}
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
FROM basedosdados-staging.br_bcb_estban_staging.municipio AS t
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}