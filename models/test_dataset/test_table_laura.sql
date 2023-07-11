{{ 
  config(
    schema='test_dataset',
    materialized='table',
    post_hook=['REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
                'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"',
                'CREATE OR REPLACE ROW ACCESS POLICY bdmais_filter 
                    ON `basedosdados-dev.br_cvm_fi.documentos_balancete`  
                    GRANT TO ("allUsers")
                    FILTER USING
                    (ano<2023)',
                 'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON `basedosdados-dev.br_cvm_fi.documentos_balancete`  
                    GRANT TO ("group:bd-pro@basedosdados.org")
                    FILTER USING
                    (ano>=2023)' )
 }}

SELECT 
SAFE_CAST(id_empreendimento AS STRING) id_empreendimento,
SAFE_CAST(data_inicio_empreendimento AS DATE) data_inicio,
SAFE_CAST(data_fim_empreendimento AS DATE) data_fim,
SAFE_CAST(finalidade AS STRING) finalidade,
SAFE_CAST(atividade AS STRING) atividade22,
SAFE_CAST(modalidade AS STRING) modalidade,
SAFE_CAST(produto AS STRING) produto,
SAFE_CAST(variedade AS STRING) variedade,
SAFE_CAST(cesta_safra AS STRING) cesta_safra,
SAFE_CAST(zoneamento AS STRING) zoneamento,
SAFE_CAST(unidade_medida AS STRING) unidade_medida,
SAFE_CAST(unidade_medida_previsao_producao AS STRING) unidade_medida_previsao_producao,
SAFE_CAST(consorcio AS STRING) consorcio,
SAFE_CAST(cedula_mae AS STRING) cedula_mae,
SAFE_CAST(id_tipo_cultura AS STRING) id_tipo_cultura
FROM basedosdados-dev.test_dataset_staging.test_table_laura AS t



