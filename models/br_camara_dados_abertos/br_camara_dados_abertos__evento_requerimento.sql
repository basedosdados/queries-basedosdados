{{ config(alias='evento_requerimento',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(idEvento AS STRING) id,
SAFE_CAST(uriEvento AS STRING) url,
SAFE_CAST(tituloRequerimento AS STRING) titulo_requerimento,
SAFE_CAST(uriRequerimento AS STRING) url_requerimento,
FROM basedosdados-staging.br_camara_dados_abertos_staging.evento_requerimento AS t