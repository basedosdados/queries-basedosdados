{{ config(alias="frente", schema="br_camara_dados_abertos") }}
select
    safe_cast(id as string) id,
    safe_cast(uri as string) url,
    safe_cast(titulo as string) titulo,
    safe_cast(datacriacao as date) data_criacao,
    safe_cast(idlegislatura as string) id_legislatura,
    safe_cast(telefone as string) telefone,
    safe_cast(situacao as string) situacao,
    safe_cast(urldocumento as string) url_documento,
    safe_cast(coordenador_id as string) id_coordenador,
    safe_cast(coordenador_nome as string) nome_coordenador,
    safe_cast(coordenador_urlfoto as string) url_foto_coordenador,
    safe_cast(coordenador_uri as string) url_coordenador,
    safe_cast(coordenador_siglauf as string) sigla_uf_coordenador,
    safe_cast(coordenador_idlegislatura as string) id_legislatura_coordenador,
    safe_cast(coordenador_siglapartido as string) sigla_partido_coordenador,
    safe_cast(coordenador_uripartido as string) url_partido_coordenador,
from `basedosdados-staging.br_camara_dados_abertos_staging.frente` as t

