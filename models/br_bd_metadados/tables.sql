SELECT
  SAFE_CAST(dataset_id AS STRING) dataset_id,
  SAFE_CAST(id AS STRING) id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(date_created AS DATE) date_created,
  SAFE_CAST(date_last_modified AS DATE) date_last_modified,
  SAFE_CAST(spatial_coverage AS STRING) spatial_coverage,
  SAFE_CAST(temporal_coverage AS STRING) temporal_coverage,
  SAFE_CAST(update_frequency AS STRING) update_frequency,
  SAFE_CAST(observation_level AS STRING) observation_level,
  SAFE_CAST(number_rows AS INT64) number_rows,
  SAFE_CAST(number_columns AS INT64) number_columns,
  SAFE_CAST(outdated AS INT64) outdated
FROM `basedosdados-dev.br_bd_metadados_staging.tables` AS t