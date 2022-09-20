SELECT
  SAFE_CAST(resource_id AS STRING) resource_id,
  SAFE_CAST(dataset_id AS STRING) dataset_id,
  SAFE_CAST(table_id AS STRING) table_id,
  SAFE_CAST(temporal_coverage AS STRING) temporal_coverage,
  SAFE_CAST(update_frequency AS STRING) update_frequency,
  SAFE_CAST(outdated AS STRING) outdated,
  SAFE_CAST(date_created AS DATE) date_created,
  SAFE_CAST(date_last_modified AS DATE) date_last_modified,
  SAFE_CAST(number_rows AS INT64) number_rows,
  SAFE_CAST(number_columns AS INT64) number_columns,
FROM `basedosdados-dev.br_bd_indicadores_staging.data_quality`