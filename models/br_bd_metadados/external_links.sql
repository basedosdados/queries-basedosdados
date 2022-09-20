SELECT
  SAFE_CAST(dataset_id AS STRING) dataset_id,
  SAFE_CAST(id AS STRING) id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(date_created AS date) date_created,
  SAFE_CAST(date_last_modified AS date) date_last_modified,
  SAFE_CAST(url AS STRING) url,
  SAFE_CAST(language AS STRING) language,
  SAFE_CAST(has_structured_data AS STRING) has_structured_data,
  SAFE_CAST(has_api AS STRING) has_api,
  SAFE_CAST(is_free AS STRING) is_free,
  SAFE_CAST(requires_registration AS STRING) requires_registration,
  SAFE_CAST(availability AS STRING) availability,
  SAFE_CAST(spatial_coverage AS STRING) spatial_coverage,
  SAFE_CAST(temporal_coverage AS STRING) temporal_coverage,
  SAFE_CAST(update_frequency AS STRING) update_frequency,
  SAFE_CAST(observation_level AS STRING) observation_level
FROM `basedosdados-dev.br_bd_metadados_staging.external_links` AS t