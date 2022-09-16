SELECT 
  SAFE_CAST(id AS STRING) id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(description AS STRING) description,
  SAFE_CAST(display_name AS STRING) display_name,
  SAFE_CAST(title AS STRING) title,
  SAFE_CAST(package_count AS INT64) package_count,
  SAFE_CAST(date_created AS DATE) date_created,
FROM `basedosdados-dev.br_bd_metadados_staging.organizations` AS t