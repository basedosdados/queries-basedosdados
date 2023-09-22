{{ config(alias='densidade_uf', schema='br_anatel_telefonia_movel',
          post_hook = [
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                        ON {{this}}
                        GRANT TO ("allUsers")
                        FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                        ON  {{this}}
                        GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                        FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)']) }}
    
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(densidade AS FLOAT64) densidade
FROM basedosdados-staging.br_anatel_telefonia_movel_staging.densidade_uf AS t 