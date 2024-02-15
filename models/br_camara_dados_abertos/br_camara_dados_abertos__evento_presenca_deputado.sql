{{ config(alias='evento_presenca_deputado',schema='br_camara_dados_abertos') }}
SELECT
DISTINCT
SAFE_CAST(idEvento AS STRING) id,
SAFE_CAST(uriEvento AS STRING) url,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataHoraInicio)), 'T')[OFFSET(0)] AS DATE) data_inicio,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataHoraInicio)), 'T')[OFFSET(1)] AS TIME) horario_inicio,
SAFE_CAST(idDeputado AS STRING) id_deputado,
SAFE_CAST(uriDeputado AS STRING) url_deputado,
FROM basedosdados-staging.br_camara_dados_abertos_staging.evento_presenca_deputado AS t

