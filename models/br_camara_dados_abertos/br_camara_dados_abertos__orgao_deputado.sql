{{ config(alias='orgao_deputado',schema='br_camara_dados_abertos') }}
SELECT DISTINCT
SAFE_CAST(nomeOrgao AS STRING) nome,
SAFE_CAST(siglaOrgao AS STRING) sigla,
SAFE_CAST(uriOrgao AS STRING) url,
SAFE_CAST(nomeDeputado AS STRING) nome_deputado,
SAFE_CAST(cargo AS STRING) cargo,
SAFE_CAST(siglaUF AS STRING) sigla_uf,
SAFE_CAST(dataInicio AS DATE) data_inicio,
SAFE_CAST(dataFim AS DATE) data_final,
SAFE_CAST(siglaPartido AS STRING) sigla_partido,
SAFE_CAST(uriDeputado AS STRING) url_deputado
FROM basedosdados-staging.br_camara_dados_abertos_staging.orgao_deputado AS t

