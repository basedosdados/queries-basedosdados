SELECT 
  SAFE_CAST(organization_id AS STRING) organization_id,
  SAFE_CAST(id AS STRING) id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(title AS STRING) title,
  SAFE_CAST(date_created AS DATE) date_created,
  SAFE_CAST(date_last_modified AS DATE) date_last_modified,
  SAFE_CAST(themes AS STRING) themes,
  SAFE_CAST(tags AS STRING) tags
FROM `basedosdados-dev.br_bd_metadados_staging.datasets` AS t