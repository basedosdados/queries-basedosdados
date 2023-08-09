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

WITH raw_cnes_equipe AS (
  -- 1. Retirar linhas com id_estabelecimento_cnes nulo
  SELECT *
  FROM `basedosdados-dev.br_ms_cnes_staging.equipe`
  WHERE CNES IS NOT NULL),
cnes_add_muni AS (
  -- 2. Adicionar id_municipio de 7 dígitos
  SELECT *
  FROM raw_cnes_equipe  
  LEFT JOIN (SELECT id_municipio, id_municipio_6,
  FROM `basedosdados-dev.br_bd_diretorios_brasil.municipio`) as mun
  ON raw_cnes_equipe.CODUFMUN = mun.id_municipio_6
)
--tipo_desativacao_equipe com valor 0 que não é indicado como um valor possível do campo no dicionário do cnes. 
-- pode ser NA. Em todos os anos tem valor significativo de zeros
--tipo_segmento e descricao_segmento vem juntos na tabela e nao esta presente no dicionario original
SELECT 
SAFE_CAST(ano AS INT64) AS ano,
SAFE_CAST(mes AS INT64) AS mes,
SAFE_CAST(sigla_uf AS STRING) AS sigla_uf,
SAFE_CAST(id_municipio AS STRING) AS id_municipio,
SAFE_CAST(CNES AS STRING) AS id_estabelecimento_cnes,
SAFE_CAST(ID_EQUIPE AS STRING) AS id_equipe,
SAFE_CAST(TIPO_EQP AS STRING) AS tipo_equipe,
SAFE_CAST(NOME_EQP AS STRING) AS equipe,
SAFE_CAST(NOMEAREA AS STRING) AS area,
SAFE_CAST(ID_SEGM AS STRING) AS id_segmento,
SAFE_CAST(TIPOSEGM AS STRING) AS tipo_segmento,
SAFE_CAST(DESCSEGM AS STRING) AS descricao_segmento,
--- inserir subsrt para criar ano e mes
SAFE_CAST(SUBSTR(DT_ATIVA, 1, 4) AS INT64) AS ano_ativacao_equipe,
SAFE_CAST(SUBSTR(DT_ATIVA,5,6) AS INT64) AS mes_ativacao_equipe,
SAFE_CAST(MOTDESAT AS STRING) AS motivo_desativacao_equipe,
SAFE_CAST(TP_DESAT AS STRING) AS tipo_desativacao_equipe,
SAFE_CAST(SUBSTR(DT_DESAT, 1, 4) AS INT64) AS ano_desativacao_equipe,
SAFE_CAST(SUBSTR(DT_DESAT,5,6) AS INT64) AS mes_desativacao_equipe,
SAFE_CAST(QUILOMBO AS STRING) AS indicador_atende_populacao_assistida_quilombolas,
SAFE_CAST(ASSENTAD AS STRING) AS indicador_atende_populacao_assistida_assentados,
SAFE_CAST(POPGERAL AS STRING) AS indicador_atende_populacao_assistida_geral,
SAFE_CAST(ESCOLA AS STRING) AS indicador_atende_populacao_assistida_escolares,
SAFE_CAST(INDIGENA AS STRING) AS indicador_atende_populacao_assistida_indigena,
SAFE_CAST(PRONASCI AS STRING) AS indicador_atende_populacao_assistida_pronasci,
FROM cnes_add_muni
WHERE concat(ano,mes) NOT IN ('20233','20234', '20235', '20236', '20237', '20238','20239','202310')
