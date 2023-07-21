{{ config(
    materialized='table',
    partition_by={
      "field": "data_consulta",
      "data_type": "date",
      "granularity": "day"
    }
)}}

WITH tabela AS (
  SELECT 
    PARSE_DATE('%Y-%m-%d', FORMAT_TIMESTAMP('%Y-%m-%d', data_hora)) AS data_consulta,
    PARSE_TIME('%H:%M:%S', FORMAT_TIMESTAMP('%H:%M:%S', data_hora)) AS hora_consulta,
    secao_site,
    item_id as id_item,
    titulo,
    vendedor_id as id_vendedor,
    CASE 
      WHEN vendedor = 'None' THEN NULL 
      ELSE vendedor 
    END AS vendedor,
    REGEXP_EXTRACT(categoria, r'^([^,]+)') AS categoria_principal, 
    REGEXP_EXTRACT(categoria, r'^([^\n]+)') AS categorias,
    envio_pais as envio_nacional,
    caracteristicas,
    quantidade_avaliacoes as quantidade_avaliacao,
    estrelas as avaliacao,
    CASE 
      WHEN IS_NAN(preco_original) THEN  null
      WHEN preco > preco_original THEN preco
      ELSE preco_original
    END preco_original,
    desconto,
    CASE
      WHEN preco > preco_original THEN preco_original
      WHEN preco = preco_original THEN  null
      ELSE preco
    END AS preco_final

  FROM 
  (
    SELECT
      ARRAY_TO_STRING(categorias, ', ') AS categoria,   
      *
    FROM
      `basedosdados.br_mercadolivre_ofertas.item` 
    WHERE NOT  ARRAY_TO_STRING(categorias, ', ')  LIKE '...%'
  ) a  
LEFT JOIN
(SELECT
  DISTINCT
    dia,
    vendedor_id,
    nome
FROM
    `basedosdados.br_mercadolivre_ofertas.vendedor`)  b
ON a.vendedor = b.nome and FORMAT_TIMESTAMP('%Y-%m-%d', data_hora) = FORMAT_TIMESTAMP('%Y-%m-%d', dia)
)
SELECT 
  data_consulta,
  hora_consulta,
  secao_site,
  id_item,
  titulo,
  id_vendedor,
  vendedor,
  categoria_principal,
  categorias,
  caracteristicas,
  envio_nacional,
  quantidade_avaliacao,
  avaliacao, 
  ROUND(
    CASE 
      WHEN preco_original IS NULL THEN preco_final * 100 / (100 - desconto)
      ELSE preco_original
    END, 2
  ) AS preco_original,
  CAST(
    CASE
      WHEN desconto IS NULL THEN 100 - (preco_final * 100 / preco_original)
      ELSE desconto
    END AS INT
  ) AS desconto,
  ROUND(
    CASE
      WHEN preco_final IS NULL THEN preco_original * (100 - desconto) / 100
      ELSE preco_final
    END, 2
  ) AS preco_final
FROM tabela
WHERE NOT (preco_original IS NULL AND preco_final IS NULL)
  AND NOT (preco_final IS NULL AND desconto IS NULL)
  AND NOT (preco_original IS NULL AND desconto IS NULL)