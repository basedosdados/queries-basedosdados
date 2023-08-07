{{ config(
    materialized='table',
    partition_by={
      "field": "data_consulta",
      "data_type": "date",
      "granularity": "day"
    }
)}}

WITH main AS (
  SELECT LPAD(id_vendor, 12, '0') as id_vendedor,
  dia,
  nome,
  SAFE_CAST(experiencia AS INT64) experiencia,
  reputacao,
  CASE
    WHEN classificacao='None' THEN NULL
    ELSE classificacao
  END AS classificacao,
  id_municipio,
  from `basedosdados-staging.br_mercadolivre_ofertas_staging.vendedor`
), predata AS (
  SELECT
    LPAD(id_vendor, 12, '0') as id_vendedor,
    STRUCT(
    json_extract_scalar(opinioes, '$.Bom') as Bom,
    json_extract_scalar(opinioes, '$.Regular') as Regular,
    json_extract_scalar(opinioes, '$.Ruim') as Ruim
    ) as opinioes
  from `basedosdados-staging.br_mercadolivre_ofertas_staging.vendedor`
), tabela_ordenada AS (
SELECT 
  dia AS data_consulta,
  id_municipio,
  main.id_vendedor,
  nome AS vendedor,
  classificacao,
  reputacao,
  experiencia AS anos_experiencia,
  SAFE_CAST(predata.opinioes.bom AS INT64) as avaliacao_bom,
  SAFE_CAST(predata.opinioes.regular AS INT64) as avaliacao_regular,
  SAFE_CAST(predata.opinioes.regular AS INT64) AS avaliacao_ruim 
FROM main
LEFT JOIN predata 
ON main.id_vendedor = predata.id_vendedor),

tabela_deduplicada AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', data_consulta) AS data_consulta,
        id_municipio,
        id_vendedor,
        vendedor,
        classificacao,
        reputacao,
        anos_experiencia,        
        ARRAY_AGG(avaliacao_bom)[OFFSET(0)] AS avaliacao_bom,
        ARRAY_AGG(avaliacao_regular)[OFFSET(0)] AS avaliacao_regular,
        ARRAY_AGG(avaliacao_ruim)[OFFSET(0)] AS avaliacao_ruim
    FROM
        tabela_ordenada
    GROUP BY
        data_consulta,
        id_vendedor,
        vendedor,
        anos_experiencia,
        reputacao,
        classificacao,
        id_municipio
    HAVING
        COUNT(*) > 1
), tabela_unicos AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', data_consulta) AS data_consulta,
        id_municipio,
        id_vendedor,
        vendedor,
        classificacao,
        reputacao,
        anos_experiencia,
        ARRAY_AGG(avaliacao_bom)[OFFSET(0)] AS avaliacao_bom,
        ARRAY_AGG(avaliacao_regular)[OFFSET(0)] AS avaliacao_regular,
        ARRAY_AGG(avaliacao_ruim)[OFFSET(0)] AS avaliacao_ruim
    FROM
        tabela_ordenada
    GROUP BY
        data_consulta,
        id_vendedor,
        vendedor,
        anos_experiencia,
        reputacao,
        classificacao,
        id_municipio
    HAVING
        COUNT(*) = 1
)
SELECT * FROM tabela_unicos
UNION ALL
SELECT * FROM tabela_deduplicada

