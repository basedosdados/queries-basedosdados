{{ config(alias="microdados", schema="br_ibge_criacao_municipios") }}
select
    safe_cast(id_municipio_criado as string) id_municipio_criado,
    safe_cast(nome_municipio_criado as string) nome_municipio_criado,
    safe_cast(id_municipio_origem as string) id_municipio_origem,
    t2.nome as nome_municipio_origem,
    safe_cast(lei as string) lei,
    safe_cast(data_criacao as date) data_criacao,
    safe_cast(data_instalacao as date) data_instalacao,
from `basedosdados-staging.br_ibge_criacao_municipios_staging.microdados` as t1
join
    `basedosdados.br_bd_diretorios_brasil.municipio` as t2
    on t1.id_municipio_origem = t2.id_municipio
