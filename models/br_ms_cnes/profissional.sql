{{ 
  config(
    schema='br_ms_cnes',
    materialized='incremental',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2005,
        "end": 2024,
        "interval": 1}
     },
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
     ]   
    )
 }}
WITH raw_cnes_profissional AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.profissional`
  WHERE CNES IS NOT NULL
)

SELECT 
CAST(SUBSTR(COMPETEN, 1, 4) AS INT64) AS ano,
CAST(SUBSTR(COMPETEN, 5, 2) AS INT64) AS mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(CNES AS STRING) id_estabelecimento_cnes,
-- replace de valores de linha com 6 zeros para null. 6 zeros é valor do campo UFMUNRES que indica null
SAFE_CAST(regexp_replace(UFMUNRES, '0{6}', '') AS STRING) id_municipio_6_residencia,
SAFE_CAST(NOMEPROF AS STRING) nome,
SAFE_CAST(VINCULAC AS STRING) id_vinculo,
SAFE_CAST(REGISTRO AS STRING) id_registro_conselho,
SAFE_CAST(CONSELHO AS STRING) id_conselho,
-- replace de valores de linha com 15 zeros para null. 15 zeros é valor do campo CNS_PROF que indica null
SAFE_CAST(regexp_replace(CNS_PROF,'0{15}', '') AS STRING) cartao_nacional_saude,
SAFE_CAST(CBO AS STRING) cbo_2002,
SAFE_CAST(TERCEIRO AS STRING) indicador_estabelecimento_terceiro,
SAFE_CAST(VINCUL_C AS STRING) indicador_vinculo_contratado_sus,
SAFE_CAST(VINCUL_A AS STRING) indicador_vinculo_autonomo_sus,
SAFE_CAST(VINCUL_N AS STRING) indicador_vinculo_outros,
SAFE_CAST(PROF_SUS AS STRING) indicador_atende_sus,
SAFE_CAST(PROFNSUS AS STRING) indicador_atende_nao_sus,
SAFE_CAST(HORAOUTR AS INT64) carga_horaria_outros,
SAFE_CAST(HORAHOSP AS INT64) carga_horaria_hospitalar,
SAFE_CAST(HORA_AMB AS INT64) carga_horaria_ambulatorial
FROM raw_cnes_profissional
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}