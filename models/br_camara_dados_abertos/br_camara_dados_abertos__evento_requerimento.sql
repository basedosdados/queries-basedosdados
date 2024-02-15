{{ config(alias="evento_requerimento", schema="br_camara_dados_abertos") }}
select
    safe_cast(idevento as string) id,
    safe_cast(urievento as string) url,
    safe_cast(titulorequerimento as string) titulo_requerimento,
    safe_cast(urirequerimento as string) url_requerimento,
from `basedosdados-staging.br_camara_dados_abertos_staging.evento_requerimento` as t