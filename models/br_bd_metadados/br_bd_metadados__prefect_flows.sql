{{ config(alias='prefect_flows',schema='br_bd_metadados') }}
SELECT
SAFE_CAST(flow_group_id AS STRING) flow_group_id,
SAFE_CAST(name AS STRING) name,
DATETIME(LEFT(flow_group_flows_aggregate_aggregate_min_created,19)) created,
SAFE_CAST(version AS INT64) latest_version,
DATETIME(LEFT(created,19)) last_update,
SAFE_CAST(schedule_type AS STRING) schedule_type,
SAFE_CAST(schedule_cron AS STRING) schedule_cron,
DATETIME(TRIM(JSON_EXTRACT(schedule_start_date,'$.dt'),'"')) schedule_start_date,
SAFE_CAST(schedule_filters AS STRING) schedule_filters,
SAFE_CAST(schedule_adjustments AS STRING) schedule_adjustments,
SAFE_CAST(schedule_labels AS STRING) schedule_labels,
SAFE_CAST(schedule_parameter_defaults AS STRING) schedule_all_parameters,
SAFE_CAST(schedule_parameters_dataset_id AS STRING) schedule_parameters_dataset_id,
SAFE_CAST(schedule_parameters_table_id AS STRING) schedule_parameters_table_id,
SAFE_CAST(schedule_parameters_dbt_alias AS BOOL) schedule_parameters_dbt_alias,
SAFE_CAST(schedule_parameters_materialization_mode AS STRING) schedule_parameters_materialization_mode,
SAFE_CAST(schedule_parameters_materialize_after_dump AS BOOL) schedule_parameters_materialize_after_dump,
SAFE_CAST(schedule_parameters_update_metadata AS BOOL) schedule_parameters_update_metadata,
FROM basedosdados-staging.br_bd_metadados_staging.prefect_flows AS t

