{{ config(alias='frente_deputado',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(id AS STRING) id,
SAFE_CAST(uri AS STRING) url,
SAFE_CAST(titulo AS STRING) titulo,
SAFE_CAST(id_deputado AS STRING) id_deputado,
INITCAP(nome_deputado) nome_deputado,
SAFE_CAST(titulo_deputado AS STRING) titulo_deputado,
SAFE_CAST(sigla_uf_deputado AS STRING) sigla_uf_deputado,
SAFE_CAST(url_deputado AS STRING) url_deputado,
SAFE_CAST(url_partido_deputado AS STRING) url_partido_deputado,
SAFE_CAST(id_legislatura_deputado AS STRING) id_legislatura_deputado,
SAFE_CAST(url_foto_deputado AS STRING) url_foto_deputado,
FROM basedosdados-staging.br_camara_dados_abertos_staging.frente_deputado AS t