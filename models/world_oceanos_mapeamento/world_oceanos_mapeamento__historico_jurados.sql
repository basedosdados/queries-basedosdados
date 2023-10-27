{{ 
  config(
    alias = 'historico_jurados',
    schema='world_oceanos_mapeamento',
    materialized='table' )
 }}
SELECT
SAFE_CAST(ano AS INTEGER) ano,
SAFE_CAST(nome_normalizado AS STRING) nome,
SAFE_CAST(nome_pais AS STRING) nome_pais,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(nome_municipio_origem AS STRING) nome_municipio_origem,
SAFE_CAST(nome_municipio_moradia AS STRING) nome_municipio_moradia,
SAFE_CAST(genero AS STRING) genero,
SAFE_CAST(ocupacao_match_1 AS STRING) ocupacao,
SAFE_CAST(instituicao AS STRING) instituicao,
SAFE_CAST(categoria AS STRING) categoria,
SAFE_CAST(indicador_juri_intermediario AS FLOAT64) indicador_juri_intermediario,
SAFE_CAST(indicador_juri_final AS FLOAT64) indicador_juri_final,
FROM basedosdados-staging.world_oceanos_mapeamento_staging.historico_jurados AS t