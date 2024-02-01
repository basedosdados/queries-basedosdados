{{
  config(
    alias = 'votacao_parlamentar',
    schema='br_camara_dados_abertos',
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2003,
        "end": 2022,
        "interval": 1}
    },    
    post_hook = ['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), week) > 6)',
          'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                ON  {{this}}
                GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), week) <= 6)']
    )
}}
SELECT 
  SAFE_CAST(ano AS INT64) ano,
  SAFE_CAST(id_votacao AS STRING) id_votacao,
  SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(data_hora)), 'T')[OFFSET(0)] AS DATE) data,
  SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(data_hora)), 'T')[OFFSET(1)] AS TIME) horario,
  SAFE_CAST(voto AS STRING) voto,
  SAFE_CAST(REPLACE(id_deputado, ".0", "") AS STRING) id_deputado,
  SAFE_CAST(nome AS STRING) nome,
  SAFE_CAST(sigla_partido AS STRING) sigla_partido,
  SAFE_CAST(sigla_uf AS STRING) sigla_uf,
  SAFE_CAST(id_legislatura AS STRING) id_legislatura
FROM basedosdados-staging.br_camara_dados_abertos_staging.votacao_parlamentar AS t