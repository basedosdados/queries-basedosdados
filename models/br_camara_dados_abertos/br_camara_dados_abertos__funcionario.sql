{{ config(alias='funcionario',schema='br_camara_dados_abertos') }}
SELECT
DISTINCT
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(cargo AS STRING) cargo,
SAFE_CAST(funcao AS STRING) funcao,
SAFE_CAST(dataInicioHistorico AS DATE) data_inicio_historico,
SAFE_CAST(dataNomeacao AS DATE) data_nomeacao,
SAFE_CAST(dataPubNomeacao AS DATE) data_publicacao_nomeacao,
SAFE_CAST(grupo AS STRING) grupo,
SAFE_CAST(ponto AS STRING) ponto,
SAFE_CAST(atoNomeacao AS STRING) ato_nomeacao,
SAFE_CAST(lotacao AS STRING) lotacao,
SAFE_CAST(uriLotacao AS STRING) url_lotacao,
FROM basedosdados-staging.br_camara_dados_abertos_staging.funcionario AS t