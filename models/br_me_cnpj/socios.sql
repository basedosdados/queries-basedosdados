{{
  config(
    schema='br_me_cnpj',
    materialized='incremental',
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
WITH cnpj_socios AS (SELECT 
    SAFE_CAST(data AS DATE) data,
    SAFE_CAST(lpad(cnpj_basico, 8, '0') AS STRING) cnpj_basico,
    SAFE_CAST(tipo AS STRING) tipo,
    SAFE_CAST(nome AS STRING) nome,
    SAFE_CAST(documento AS STRING) documento,
    SAFE_CAST(CAST(qualificacao AS INT64) AS STRING) qualificacao,
    SAFE_CAST(data_entrada_sociedade AS DATE) data_entrada_sociedade,
    SAFE_CAST(CAST(id_pais AS INT64) AS STRING) id_pais,
    SAFE_CAST(cpf_representante_legal AS STRING) cpf_representante_legal,
    SAFE_CAST(nome_representante_legal AS STRING) nome_representante_legal,
    SAFE_CAST(CAST(qualificacao_representante_legal AS INT64) AS STRING) qualificacao_representante_legal,
    SAFE_CAST(faixa_etaria AS STRING) faixa_etaria
FROM basedosdados-staging.br_me_cnpj_staging.socios AS t
WHERE qualificacao != "qualificacao")
SELECT * FROM cnpj_socios
{% if is_incremental() %} 
WHERE data > (SELECT MAX(data) FROM {{ this }} )
{% endif %}