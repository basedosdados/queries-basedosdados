{{
    config(
        alias="coordenada_endereco",
        schema="br_ibge_censo_2022",
        materialized="table",
        cluster_by=["id_municipio", "id_uf", "especie_endereco"],
    )
}}

select
    safe_cast(id_uf as string) id_uf,
    safe_cast(cod_mun as string) id_municipio,
    safe_cast(cod_especie as string) especie_endereco,
    safe_cast(nv_geo_coord as string) nivel_geo_coordenada,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    st_geogpoint(safe_cast(longitude as float64), safe_cast(latitude as float64)) ponto
from `basedosdados-staging.br_ibge_censo_2022_staging.coordenada_endereco ` as t
