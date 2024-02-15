{{ config(alias="evento_orgao", schema="br_camara_dados_abertos") }}
select
    safe_cast(idevento as string) id,
    safe_cast(urievento as string) url,
    safe_cast(idorgao as string) id_orgao,
    safe_cast(siglaorgao as string) sigla_orgao,
    safe_cast(uriorgao as string) url_orgao,
from `basedosdados-staging.br_camara_dados_abertos_staging.evento_orgao` as t