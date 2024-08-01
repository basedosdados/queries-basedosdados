{{ config(alias="areas", schema="br_rf_cno", materialized="table") }}

select
    safe_cast(data as date) data_extracao,
    safe_cast(id_cno as string) id_cno,
    safe_cast(categoria as string) categoria,
    safe_cast(destinacao as string) destinacao,
    safe_cast(tipo_obra as string) tipo_obra,
    safe_cast(tipo_area as string) tipo_area,
    safe_cast(tipo_area_complementar as string) tipo_area_complementar,
    safe_cast(metragem as float64) metragem,
from `basedosdados-staging.br_rf_cno_staging.areas` as t
