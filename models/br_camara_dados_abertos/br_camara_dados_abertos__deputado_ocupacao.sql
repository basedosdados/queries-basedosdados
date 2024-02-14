{{ config(alias="deputado_ocupacao", schema="br_camara_dados_abertos") }}
select
    safe_cast(ano_inicio as int64) ano_inicio,
    safe_cast(ano_fim as int64) ano_fim,
    safe_cast(id_deputado as string) id_deputado,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(entidade as string) entidade,
    safe_cast(titulo as string) titulo,
from `basedosdados-staging.br_camara_dados_abertos_staging.deputado_ocupacao ` as t
