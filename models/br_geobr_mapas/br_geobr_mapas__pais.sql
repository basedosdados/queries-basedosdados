{{
    config(
        alias="pais",
        schema="br_geobr_mapas",
        materialized="table",
    )
}}
select safe.st_geogfromtext(geometria) geometria,
from `basedosdados-staging.br_geobr_mapas_staging.pais` as t
