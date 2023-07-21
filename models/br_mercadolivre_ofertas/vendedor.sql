{{ config(
    materialized='table',
    partition_by={
      "field": "data_consulta",
      "data_type": "date",
      "granularity": "day"
    }
)}}


WITH tabela_deduplicada AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', dia)) AS data_consulta,
        id_municipio,
        vendedor_id as id_vendedor,
        nome as vendedor,
        classificacao,
        reputacao,
        experiencia as anos_experiencia,        
        ARRAY_AGG(opinioes.Bom)[OFFSET(0)] AS avaliacao_bom,
        ARRAY_AGG(opinioes.Regular)[OFFSET(0)] AS avaliacao_regular,
        ARRAY_AGG(opinioes.Ruim)[OFFSET(0)] AS avaliacao_ruim
    FROM
        `basedosdados.br_mercadolivre_ofertas.vendedor`
    GROUP BY
        data_consulta,
        vendedor_id,
        vendedor,
        experiencia,
        reputacao,
        classificacao,
        id_municipio
    HAVING
        COUNT(*) > 1
), tabela_unicos AS (
    SELECT
        PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', dia)) AS data_consulta,
        id_municipio,
        vendedor_id as id_vendedor,
        nome as vendedor,
        classificacao,
        reputacao,
        experiencia as anos_experiencia,
        ARRAY_AGG(opinioes.Bom)[OFFSET(0)] AS avaliacao_bom,
        ARRAY_AGG(opinioes.Regular)[OFFSET(0)] AS avaliacao_regular,
        ARRAY_AGG(opinioes.Ruim)[OFFSET(0)] AS avaliacao_ruim
    FROM
        `basedosdados.br_mercadolivre_ofertas.vendedor`
    GROUP BY
        data_consulta,
        vendedor_id,
        vendedor,
        experiencia,
        reputacao,
        classificacao,
        id_municipio
    HAVING
        COUNT(*) = 1
)
SELECT 
    data_consulta,
    id_municipio,
    id_vendedor,
    vendedor,
    classificacao,
    reputacao,
    anos_experiencia,
    SAFE_CAST(avaliacao_bom AS INT64) AS avaliacao_bom,
    SAFE_CAST(avaliacao_regular AS INT64) AS avaliacao_regular,
    SAFE_CAST(avaliacao_ruim AS INT64) AS avaliacao_ruim    
FROM tabela_unicos
UNION ALL
SELECT 
    data_consulta,
    id_municipio,
    id_vendedor,
    vendedor,
    classificacao,
    reputacao,
    anos_experiencia,
    SAFE_CAST(avaliacao_bom AS INT64) AS avaliacao_bom,
    SAFE_CAST(avaliacao_regular AS INT64) AS avaliacao_regular,
    SAFE_CAST(avaliacao_ruim AS INT64) AS avaliacao_ruim   
FROM tabela_deduplicada

