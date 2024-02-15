{{ config(alias='evento',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(id AS STRING) id,
SAFE_CAST(uri AS STRING) url,
SAFE_CAST(urlDocumentoPauta AS STRING) url_documento_pauta,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataHoraInicio)), 'T')[OFFSET(0)] AS DATE) data_inicio,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataHoraInicio)), 'T')[OFFSET(1)] AS TIME) horario_inicio,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataHoraFim)), 'T')[OFFSET(0)] AS DATE) data_final,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataHoraFim)), 'T')[OFFSET(1)] AS TIME) horario_final,
SAFE_CAST(situacao AS STRING) situacao,
SAFE_CAST(descricao AS STRING) descricao,
SAFE_CAST(descricaoTipo AS STRING) tipo,
SAFE_CAST(localExterno AS STRING) local_externo,
SAFE_CAST(localCamara_nome AS STRING) nome_local,
FROM basedosdados-staging.br_camara_dados_abertos_staging.evento AS t