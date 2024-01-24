{{ config(alias='legislatura',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(anoEleicao AS INT64) ano,
SAFE_CAST(idLegislatura AS STRING) id,
SAFE_CAST(uri AS STRING) url,
SAFE_CAST(dataInicio AS DATE) data_inicio,
SAFE_CAST(dataFim AS DATE) data_final,
FROM basedosdados-staging.br_camara_dados_abertos_staging.legislatura AS t

