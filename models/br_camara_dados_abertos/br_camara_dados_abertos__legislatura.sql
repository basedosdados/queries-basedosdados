{{ config(alias="legislatura", schema="br_camara_dados_abertos") }}
select
    safe_cast(anoeleicao as int64) ano,
    safe_cast(idlegislatura as string) id,
    safe_cast(uri as string) url,
    safe_cast(datainicio as date) data_inicio,
    safe_cast(datafim as date) data_final,
from `basedosdados-staging.br_camara_dados_abertos_staging.legislatura` as t
