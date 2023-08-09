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
     }  
    )
 }}

WITH raw_cnes_profissional AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-staging.br_ms_cnes_staging.profissional`
  WHERE CNES IS NOT NULL
),
profissional_x_estabelecimento as(
  SELECT *
  FROM raw_cnes_profissional as pf
  LEFT JOIN (
    SELECT id_municipio, 
    CAST(ano AS STRING) as ano1, 
    CAST(mes AS STRING) as mes1, 
    id_estabelecimento_cnes AS IDDD 
    FROM `basedosdados.br_ms_cnes.estabelecimento`) as st
  ON pf.CNES = st.IDDD AND pf.ano = st.ano1 AND pf.mes = st.mes1
)
SELECT 
CAST(SUBSTR(COMPETEN, 1, 4) AS INT64) AS ano,
CAST(SUBSTR(COMPETEN, 5, 2) AS INT64) AS mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
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
FROM profissional_x_estabelecimento 
WHERE concat(ano,mes) NOT IN ('20233','20234', '20235', '20236', '20237', '20238','20239','202310')
