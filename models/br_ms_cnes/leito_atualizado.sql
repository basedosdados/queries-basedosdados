{{ 
  config(
    schema='br_ms_cnes',
    materialized='table',
     partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2005,
        "end": 2023,
        "interval": 1}
     },
    post_hook=[
      'REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
      'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"'
    ])  
 }}


WITH raw_cnes_leito AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.leito`
  WHERE CNES IS NOT NULL),
cnes_leito_without_duplicates AS (
  --2. Remover linhas duplicadas
    SELECT DISTINCT *
    FROM raw_cnes_leito
),
leito_x_estabelecimento as(
  --3. Adicionar id_municipio de 7 dígitos fazendo join com a tabela estabalecimento
  -- ps: a coluna id_municipio não vem por padrão na tabela leito extraída do FTP do Datasus
  SELECT *
  FROM cnes_leito_without_duplicates as lt
  LEFT JOIN (SELECT id_municipio, ano, mes, id_estabelecimento_cnes,id_municipio AS IDDD from `basedosdados-dev.br_ms_cnes.estabelecimento`) as st
  ON lt.id_estabelecimento_cnes = st.IDDD AND lt.ano = st.ano AND lt.mes = st.mes 
)

SELECT 
SAFE_CAST(ano AS INT64) AS ano,
SAFE_CAST(mes AS INT64) AS mes,
SAFE_CAST(sigla_uf AS STRING) AS sigla_uf,
SAFE_CAST(id_municipio AS STRING) AS id_municipio,
SAFE_CAST(CNES AS STRING) AS id_estabelecimento_cnes,
SAFE_CAST(CODLEITO AS STRING) AS tipo_especialidade_leito,
SAFE_CAST(TP_LEITO AS STRING) AS tipo_leito,
SAFE_CAST(QT_EXIST AS STRING) AS quantidade_total,
SAFE_CAST(QT_CONTR AS STRING) AS quantidade_contratado,
SAFE_CAST(QT_SUS AS STRING) AS quantidade_sus
FROM leito_x_estabelecimento