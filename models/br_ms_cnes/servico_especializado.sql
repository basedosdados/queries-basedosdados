{{ 
  config(
    schema='br_ms_cnes',
    materialized='incremental',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2005,
        "end": 2023,
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
WITH raw_cnes_servico_especializado AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.servico_especializado`
  WHERE CNES IS NOT NULL
),
raw_cnes_servico_especializado_without_duplicates as(
  -- 2. distinct nas linhas 
  SELECT DISTINCT *
  FROM raw_cnes_servico_especializado
),
cnes_add_muni AS (
  -- 3. Adicionar id_municipio e sigla_uf
  SELECT *
  FROM raw_cnes_servico_especializado_without_duplicates  
  LEFT JOIN (SELECT id_municipio, id_municipio_6,
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) as mun
  ON raw_cnes_servico_especializado_without_duplicates.CODUFMUN = mun.id_municipio_6
)

SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(CNES AS STRING) id_estabelecimento_cnes,
SAFE_CAST(SERV_ESP AS STRING) tipo_servico_especializado,
SAFE_CAST(CLASS_SR AS STRING) tipo_classificacao,
SAFE_CAST(CONCAT(SERV_ESP, CLASS_SR) AS STRING) tipo_classificacao_bd,
SAFE_CAST(SRVUNICO AS STRING) tipo_servico_especializado_unico,
SAFE_CAST(CARACTER AS STRING) tipo_caracterizacao,
SAFE_CAST(AMB_NSUS AS INT64) indicador_servico_ambulatorial_sus,
SAFE_CAST(AMB_SUS AS INT64) indicador_servico_nao_sus,
SAFE_CAST(HOSP_NSUS AS INT64) indicador_servico_hospitalar_nao_sus,
SAFE_CAST(HOSP_SUS AS INT64) indicador_servico_hospitalar_sus,
SAFE_CAST(CONTSRVU AS INT64) indicador_servico_especializado_unico,
SAFE_CAST(CNESTERC AS INT64) quantidade_nacional_estabelecimento_saude_terceiro
FROM cnes_add_muni AS t
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}