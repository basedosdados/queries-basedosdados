{{ config(alias='deputado_profissao',schema='br_camara_dados_abertos') }}
SELECT
    SAFE_CAST(id_deputado AS INT64) id_deputado,
    SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(data)), 'T')[OFFSET(0)] AS DATE) data,
    SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(data)), 'T')[OFFSET(1)] AS TIME) horario,
    SAFE_CAST(id_profissao AS STRING) id_profissao,
    SAFE_CAST(titulo AS STRING) titulo,
FROM basedosdados-staging.br_camara_dados_abertos_staging.deputado_profissao AS t