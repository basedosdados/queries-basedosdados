{{ 
  config(
    alias = 'diaria',
    schema='br_bcb_taxa_cambio',
    materialized='table',
    labels = {'tema': 'economia'})
 }}
SELECT 
SAFE_CAST(ano AS STRING) ano,
SAFE_CAST(data_cotacao AS DATE) data_cotacao,
SAFE_CAST(hora_cotacao AS TIME) hora_cotacao,
SAFE_CAST(moeda AS STRING) moeda,
SAFE_CAST(tipo_moeda AS STRING) tipo_moeda,
SAFE_CAST(tipo_boletim AS STRING) tipo_boletim,
SAFE_CAST(paridade_compra AS FLOAT64) paridade_compra,
SAFE_CAST(paridade_venda AS FLOAT64) paridade_venda,
SAFE_CAST(cotacao_compra AS FLOAT64) cotacao_compra,
SAFE_CAST(cotacao_venda AS FLOAT64) cotacao_venda
FROM basedosdados-staging.br_bcb_taxa_cambio_staging.taxa_cambio AS t