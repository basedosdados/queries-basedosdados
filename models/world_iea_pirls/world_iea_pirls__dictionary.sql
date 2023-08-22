{{ config(alias='dictionary', schema='world_iea_pirls') }}

SELECT 
SAFE_CAST(table_id AS STRING) table_id,
SAFE_CAST(column_name AS STRING) column_name,
SAFE_CAST(key AS STRING) key,
SAFE_CAST(temporal_coverage AS STRING) temporal_coverage,
SAFE_CAST(value AS STRING) value
FROM basedosdados-staging.world_iea_pirls_staging.dicionario AS t
