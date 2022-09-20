SELECT
  SAFE_CAST(dataset_id AS STRING) dataset_id,
  SAFE_CAST(id AS STRING) id,
  SAFE_CAST(name AS STRING) name,
  SAFE_CAST(date_created AS DATE) date_created,
  SAFE_CAST(date_last_modified AS DATE) date_last_modified,
  SAFE_CAST(url AS STRING) url,
  SAFE_CAST(origin AS STRING) origin,
  SAFE_CAST(number AS STRING) number,
  SAFE_CAST(opening_date AS DATE) opening_date,
  SAFE_CAST(requested_by AS STRING) requested_by,
  SAFE_CAST(status AS STRING) status,
  SAFE_CAST(data_url AS STRING) data_url,
  SAFE_CAST(spatial_coverage AS STRING) spatial_coverage,
  SAFE_CAST(temporal_coverage AS STRING) temporal_coverage,
  SAFE_CAST(update_frequency AS STRING) update_frequency,
  SAFE_CAST(observation_level AS STRING) observation_level
FROM `basedosdados-dev.br_bd_metadados_staging.information_requests` AS t