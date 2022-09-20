SELECT
  SAFE_CAST(dataset_id AS STRING) dataset_id,
  SAFE_CAST(id AS STRING) id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(date_created AS DATE) date_created,
  SAFE_CAST(date_last_modified AS DATE) date_last_modified,
  SAFE_CAST(type AS STRING) type
FROM `basedosdados-dev.br_bd_metadados_staging.resources` AS t