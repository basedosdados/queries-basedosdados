{{ config(alias='legislatura_mesa',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(idLegislatura AS STRING) id,
SAFE_CAST(dataInicio AS DATETIME) data_inicio,
SAFE_CAST(dataFim AS DATETIME) data_final,
SAFE_CAST(idOrgao AS STRING) id_orgao,
SAFE_CAST(uriOrgao AS STRING) url_orgao,
SAFE_CAST(siglaOrgao AS STRING) sigla_orgao,
SAFE_CAST(nomeOrgao AS STRING) nome_orgao,
SAFE_CAST(idDeputado AS STRING) id_deputado,
SAFE_CAST(nomeDeputado AS STRING) nome_deputado,
SAFE_CAST(cargo AS STRING) cargo,
SAFE_CAST(uriDeputado AS STRING) url_deputado,
SAFE_CAST(siglaPartido AS STRING) sigla_partido,
SAFE_CAST(siglaUF AS STRING) sigla_uf,
FROM basedosdados-staging.br_camara_dados_abertos_staging.legislatura_mesa AS t

