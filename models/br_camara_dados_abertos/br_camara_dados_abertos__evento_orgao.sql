{{ config(alias='evento_orgao',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(idEvento AS STRING) id,
SAFE_CAST(uriEvento AS STRING) url,
SAFE_CAST(idOrgao AS STRING) id_orgao,
SAFE_CAST(siglaOrgao AS STRING) sigla_orgao,
SAFE_CAST(uriOrgao AS STRING) url_orgao,
FROM basedosdados-staging.br_camara_dados_abertos_staging.evento_orgao AS t

