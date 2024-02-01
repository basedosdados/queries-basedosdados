{{ config(alias='orgao',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(apelido AS STRING) apelido,
SAFE_CAST(sigla AS STRING) sigla,
SAFE_CAST(uri AS STRING) url_orgao,
SAFE_CAST(tipoOrgao AS STRING) tipo_orgao,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataInicio)), 'T')[OFFSET(0)] AS DATE) data_inicio,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataInstalacao)), 'T')[OFFSET(0)] AS DATE) data_instalacao,
SAFE_CAST(SPLIT(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', TIMESTAMP(dataFim)), 'T')[OFFSET(0)] AS DATE) data_final,
SAFE_CAST(descricaoSituacao AS STRING) situacao,
SAFE_CAST(casa AS STRING) casa,
SAFE_CAST(sala AS STRING) sala,
FROM basedosdados-staging.br_camara_dados_abertos_staging.orgao AS t

