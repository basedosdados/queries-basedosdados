{{config(alias = 'proposicao_autor',schema='br_camara_dados_abertos')}}

SELECT
    SAFE_CAST(idProposicao AS STRING) id_proposicao,
    SAFE_CAST(uriProposicao AS STRING) url_proposicao,
    REPLACE(SAFE_CAST(idDeputadoAutor AS STRING), ".0", "") id_deputado,
    INITCAP(SAFE_CAST(tipoAutor AS STRING)) tipo_autor,
    INITCAP(SAFE_CAST(nomeAutor AS STRING)) nome_autor,
    SAFE_CAST(uriAutor AS STRING) url_autor,
    SAFE_CAST(siglaPartidoAutor AS STRING) sigla_partido,
    UPPER(SAFE_CAST(SiglaUFAutor AS STRING)) sigla_uf_autor,
    SAFE_CAST(uriPartidoAutor AS STRING) url_partido,
    SAFE_CAST(ordemAssinatura AS STRING) ordem_assinatura,
    SAFE_CAST(proponente AS STRING) proponente,
FROM basedosdados-staging.br_camara_dados_abertos_staging.proposicao_autor AS t