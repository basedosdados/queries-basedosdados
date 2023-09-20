{{
  config(
    schema = "world_wb_mides",
    materialized = "table",
    partition_by = {
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2009,
        "end": 2022,
        "interval": 1}
    },
    cluster_by = ["sigla_uf"],
    labels = {"project_id": "basedosdados", "tema": "economia"}
  )
 }}
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(orgao AS STRING) orgao,
SAFE_CAST(id_unidade_gestora AS STRING) id_unidade_gestora,
SAFE_CAST(id_licitacao_bd AS STRING) id_licitacao_bd,
SAFE_CAST(id_licitacao AS STRING) id_licitacao,
SAFE_CAST(id_dispensa AS STRING) id_dispensa,
SAFE_CAST(id_item_bd AS STRING) id_item_bd,
SAFE_CAST(id_item AS STRING) id_item,
SAFE_CAST(descricao AS STRING) descricao,
SAFE_CAST(numero AS INT64) numero,
SAFE_CAST(numero_lote AS INT64) numero_lote,
SAFE_CAST(unidade_medida AS STRING) unidade_medida,
SAFE_CAST(quantidade_cotada AS INT64) quantidade_cotada,
SAFE_CAST(valor_unitario_cotacao AS FLOAT64) valor_unitario_cotacao,
SAFE_CAST(quantidade AS INT64) quantidade,
SAFE_CAST(valor_unitario AS FLOAT64) valor_unitario,
SAFE_CAST(valor_total AS FLOAT64) valor_total,
SAFE_CAST(quantidade_proposta AS INT64) quantidade_proposta,
SAFE_CAST(valor_proposta AS FLOAT64) valor_proposta,
SAFE_CAST(valor_vencedor AS FLOAT64) valor_vencedor,
SAFE_CAST(nome_vencedor AS STRING) nome_vencedor,
SAFE_CAST(documento AS STRING) documento
FROM basedosdados-staging.world_wb_mides_staging.licitacao_item AS t