{{ config(alias='bigquery_tables',schema='br_bd_metadados') }}
SELECT 
    project_id
    ,dataset_id
    ,table_id
    ,CASE
        WHEN type = '1' THEN 'table'
        WHEN type = '2' THEN 'view'
        WHEN type = '3' THEN 'external'
        ELSE 'unknown'
     END AS type
    ,DATE(TIMESTAMP_MILLIS(SAFE_CAST(creation_time AS INT64))) AS creation_date
    ,DATE(TIMESTAMP_MILLIS(SAFE_CAST(last_modified_time AS INT64))) AS last_modified_date
    ,TIMESTAMP_MILLIS(SAFE_CAST(creation_time AS INT64)) AS creation_time
    ,TIMESTAMP_MILLIS(SAFE_CAST(last_modified_time AS INT64)) AS last_modified_time
    ,SAFE_CAST(row_count AS INT64) as row_count 
    ,SAFE_CAST(size_bytes AS INT64) as size_bytes
FROM `basedosdados-staging.br_bd_metadados_staging.bigquery_tables`

