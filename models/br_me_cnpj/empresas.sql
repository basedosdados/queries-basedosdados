{{
  config(
    schema='br_me_cnpj',
    materialized='incremental',
    unique_key='data',
    partition_by={
      "field": "data",
      "data_type": "date",
    },
    pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    post_hook=['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(data), MONTH) > 6 OR  DATE_DIFF(DATE(2023,5,1),DATE(data), MONTH) > 0)',
              'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (EXTRACT(YEAR from data) = EXTRACT(YEAR from  CURRENT_DATE()))' ]) 
}}
WITH cnpj_empresas AS (SELECT 
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(lpad(cnpj_basico, 8, '0') AS STRING) cnpj_basico,
    SAFE_CAST(razao_social AS STRING) razao_social,
    SAFE_CAST(natureza_juridica AS STRING) natureza_juridica,
    SAFE_CAST(qualificacao_responsavel AS STRING) qualificacao_responsavel,
    SAFE_CAST(capital_social AS FLOAT64) capital_social,
    SAFE_CAST(REGEXP_REPLACE(porte, '^0', '') AS STRING) porte,
    SAFE_CAST(ente_federativo AS STRING) ente_federativo
FROM basedosdados-staging.br_me_cnpj_staging.empresas AS t
WHERE porte != "porte")
SELECT * FROM cnpj_empresas
{% if is_incremental() %} 
WHERE data > (SELECT MAX(data) FROM {{ this }} )
{% endif %}