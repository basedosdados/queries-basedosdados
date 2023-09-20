{{ 
  config(
    alias = 'pessoa_fisica',
    schema='br_cvm_administradores_carteira',
    materialized='incremental',
    partition_by = {
      "field": "data_registro",
      "data_type": "date",
      "granularity": "day"
     },
     pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    post_hook=['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(data_registro), MONTH) > 6)',
              'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (EXTRACT(YEAR from data_registro) = EXTRACT(YEAR from  DATE("{{ run_started_at.strftime("%Y-%m-%d") }}")))' ] 
    )
 }}
SELECT 
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(data_registro AS DATE) data_registro,
SAFE_CAST(data_cancelamento AS DATE) data_cancelamento,
SAFE_CAST(motivo_cancelamento AS STRING) motivo_cancelamento,
SAFE_CAST(situacao AS STRING) situacao,
SAFE_CAST(data_inicio_situacao AS DATE) data_inicio_situacao,
SAFE_CAST(categoria_registro AS STRING) categoria_registro
FROM basedosdados-staging.br_cvm_administradores_carteira_staging.pessoa_fisica AS t
