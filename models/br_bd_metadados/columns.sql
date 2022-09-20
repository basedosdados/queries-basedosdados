SELECT
  SAFE_CAST(table_id AS STRING) table_id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(bigquery_type AS STRING) bigquery_type,
  SAFE_CAST(description AS STRING) description,
  SAFE_CAST(temporal_coverage AS STRING) temporal_coverage,
  SAFE_CAST(covered_by_dictionary AS STRING) covered_by_dictionary,
  SAFE_CAST(directory_column AS STRING) directory_column,
  SAFE_CAST(measurement_unit AS STRING) measurement_unit,
  SAFE_CAST(has_sensitive_data AS STRING) has_sensitive_data,
  SAFE_CAST(observations AS STRING) observations,
  SAFE_CAST(is_in_staging AS STRING) is_in_staging,
  SAFE_CAST(is_partition AS STRING) is_partition
FROM `basedosdados-dev.br_bd_metadados_staging.columns` AS t