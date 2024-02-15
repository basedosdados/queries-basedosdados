{{ config(alias='frente',schema='br_camara_dados_abertos') }}
SELECT
SAFE_CAST(id AS STRING) id,
SAFE_CAST(uri AS STRING) url,
SAFE_CAST(titulo AS STRING) titulo,
SAFE_CAST(dataCriacao AS DATE) data_criacao,
SAFE_CAST(idLegislatura AS STRING) id_legislatura,
SAFE_CAST(telefone AS STRING) telefone,
SAFE_CAST(situacao AS STRING) situacao,
SAFE_CAST(urlDocumento AS STRING) url_documento,
SAFE_CAST(coordenador_id AS STRING) id_coordenador,
SAFE_CAST(coordenador_nome AS STRING) nome_coordenador,
SAFE_CAST(coordenador_urlFoto AS STRING) url_foto_coordenador,
SAFE_CAST(coordenador_uri AS STRING) url_coordenador,
SAFE_CAST(coordenador_siglaUf AS STRING) sigla_uf_coordenador,
SAFE_CAST(coordenador_idLegislatura AS STRING) id_legislatura_coordenador,
SAFE_CAST(coordenador_siglaPartido AS STRING) sigla_partido_coordenador,
SAFE_CAST(coordenador_uriPartido AS STRING) url_partido_coordenador,
FROM basedosdados-staging.br_camara_dados_abertos_staging.frente AS t

