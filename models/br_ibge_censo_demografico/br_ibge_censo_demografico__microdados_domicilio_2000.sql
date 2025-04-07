{{
    config(
        alias="microdados_domicilio_2000",
        schema="br_ibge_censo_demografico",
        materialized="table",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
    )
}}
select
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_mesorregiao as string) id_mesorregiao,
    safe_cast(id_microrregiao as string) id_microrregiao,
    safe_cast(id_regiao_metropolitana as string) id_regiao_metropolitana,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_distrito as string) id_distrito,
    safe_cast(id_subdistrito as string) id_subdistrito,
    safe_cast(controle as int64) controle,
    safe_cast(situacao_setor as int64) situacao_setor,
    safe_cast(situacao_domicilio as int64) situacao_domicilio,
    safe_cast(tipo_setor as string) tipo_setor,
    safe_cast(peso_amostral as float64) peso_amostral,
    safe_cast(area_ponderacao as int64) area_ponderacao,
    safe_cast(v0110 as int64) v0110,
    safe_cast(v0111 as int64) v0111,
    safe_cast(v0201 as string) v0201,
    safe_cast(v0202 as string) v0202,
    safe_cast(v0203 as int64) v0203,
    safe_cast(v0204 as int64) v0204,
    safe_cast(v0205 as string) v0205,
    safe_cast(v0206 as string) v0206,
    safe_cast(v0207 as string) v0207,
    safe_cast(v0208 as string) v0208,
    safe_cast(v0209 as string) v0209,
    safe_cast(v0210 as string) v0210,
    safe_cast(v0211 as string) v0211,
    safe_cast(v0212 as string) v0212,
    safe_cast(v0213 as string) v0213,
    safe_cast(v0214 as string) v0214,
    safe_cast(v0215 as string) v0215,
    safe_cast(v0216 as string) v0216,
    safe_cast(v0217 as string) v0217,
    safe_cast(v0218 as string) v0218,
    safe_cast(v0219 as string) v0219,
    safe_cast(v0220 as string) v0220,
    safe_cast(v0221 as string) v0221,
    safe_cast(v0222 as string) v0222,
    safe_cast(v0223 as string) v0223,
    safe_cast(v7100 as int64) v7100,
    safe_cast(v7203 as float64) v7203,
    safe_cast(v7204 as float64) v7204,
    safe_cast(v7401 as int64) v7401,
    safe_cast(v7402 as int64) v7402,
    safe_cast(v7403 as int64) v7403,
    safe_cast(v7404 as int64) v7404,
    safe_cast(v7405 as int64) v7405,
    safe_cast(v7406 as int64) v7406,
    safe_cast(v7407 as int64) v7407,
    safe_cast(v7408 as int64) v7408,
    safe_cast(v7409 as int64) v7409,
    safe_cast(v7616 as int64) v7616,
    safe_cast(v7617 as int64) v7617,
    safe_cast(v1111 as string) v1111,
    safe_cast(v1112 as string) v1112,
    safe_cast(v1113 as string) v1113
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_domicilio_2000"
        )
    }} t
