{{ config(alias='prefect_flow_runs',schema='br_bd_metadados') }}
SELECT
SAFE_CAST(id AS STRING) id,
SAFE_CAST(flow_flow_group_id AS STRING) flow_group_id,
SAFE_CAST(name AS STRING) name,
SAFE_CAST(labels AS STRING) labels,
SAFE_CAST(flow_project_name AS STRING) flow_project_name,
SAFE_CAST(flow_name AS STRING) flow_name,
SAFE_CAST(flow_archived AS BOOL) flow_archived,
SAFE_CAST(dataset_id AS STRING) dataset_id,
SAFE_CAST(table_id AS STRING) table_id,
DATETIME(LEFT(start_time,19)) start_time,
DATETIME(LEFT(end_time,19)) end_time,
SAFE_CAST(state AS STRING) state,
SAFE_CAST(state_message AS STRING) state_message,
SAFE_CAST(task_runs AS STRING) task_runs,
SAFE_CAST(skipped_upload_to_gcs AS BOOL) skipped_upload_to_gcs,
SAFE_CAST(logs AS STRING) error_logs,
FROM basedosdados-staging.br_bd_metadados_staging.prefect_flow_runs AS t

