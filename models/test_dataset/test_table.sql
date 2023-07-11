{{ 
  config(
    schema='test_dataset',
    materialized='table',
    post_hook=['CREATE OR REPLACE ROW ACCESS POLICY bdmais_filter 
                    ON `basedosdados.test_dataset.test_table`  
                    GRANT TO ("allUsers")
                    FILTER USING
                    (ano<2023)',
                'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON `basedosdados.test_dataset.test_table`  
                    GRANT TO ("group:bd-pro@basedosdados.org")
                    FILTER USING
                    (ano>=2023)'
                    ])
 }}

 
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(dado AS STRING) dado
FROM basedosdados-staging.test_dataset_staging.test_table AS t