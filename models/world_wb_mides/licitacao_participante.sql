{{
  config(
    schema = "world_wb_mides",
    materialized = "table",
    partition_by = {
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2009,
        "end": 2021,
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
SAFE_CAST(razao_social AS STRING) razao_social,
SAFE_CAST(documento AS STRING) documento,
SAFE_CAST(habilitado AS INT64) habilitado,
SAFE_CAST(classificado AS INT64) classificado,
SAFE_CAST(vencedor AS INT64) vencedor,
SAFE_CAST(endereco AS STRING) endereco,
SAFE_CAST(cep AS STRING) cep,
SAFE_CAST(municipio_participante AS STRING) municipio_participante,
SAFE_CAST(tipo AS STRING) tipo
FROM basedosdados-staging.world_wb_mides_staging.licitacao_participante AS t