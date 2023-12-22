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


WITH raw_cnes_equipamento AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.equipamento`
  WHERE CNES IS NOT NULL),
cnes_add_muni AS (
  -- 2. Adicionar id_municipio de 7 dígitos
  SELECT *
  FROM raw_cnes_equipamento  
  LEFT JOIN (SELECT id_municipio, id_municipio_6,
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`) as mun
  ON raw_cnes_equipamento.CODUFMUN = mun.id_municipio_6
)
SELECT 
SAFE_CAST(ano AS INT64) AS ano,
SAFE_CAST(mes AS INT64) AS mes,
SAFE_CAST(sigla_uf AS STRING) AS sigla_uf,
SAFE_CAST(id_municipio AS STRING) AS id_municipio,
SAFE_CAST(CNES AS STRING) AS id_estabelecimento_cnes,
SAFE_CAST(CODEQUIP AS STRING) AS id_equipamento,
SAFE_CAST(TIPEQUIP AS STRING) AS tipo_equipamento,
SAFE_CAST(QT_EXIST AS STRING) AS quantidade_equipamentos,
SAFE_CAST(QT_USO AS STRING) AS quantidade_equipamentos_ativos,
SAFE_CAST(IND_SUS AS INT64) AS indicador_equipamento_disponivel_sus,
SAFE_CAST(IND_NSUS AS INT64) AS indicador_equipamento_indisponivel_sus
FROM cnes_add_muni 
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}


