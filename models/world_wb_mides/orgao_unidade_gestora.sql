{{
  config(
    schema = "world_wb_mides",
    materialized = "table",
    cluster_by = ["sigla_uf"],
    labels = {"project_id": "basedosdados", "tema": "economia"}
  )
 }}
SELECT 
SAFE_CAST(ano AS STRING) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(orgao AS STRING) orgao,
SAFE_CAST(nome_orgao AS STRING) nome_orgao,
SAFE_CAST(id_unidade_gestora AS STRING) id_unidade_gestora,
SAFE_CAST(nome_unidade_gestora AS STRING) nome_unidade_gestora,
SAFE_CAST(esfera AS STRING) esfera
FROM basedosdados-staging.world_wb_mides_staging.orgao_unidade_gestora AS t