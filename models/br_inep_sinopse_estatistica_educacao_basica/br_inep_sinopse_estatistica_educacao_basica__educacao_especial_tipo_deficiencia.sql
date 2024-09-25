{{
    config(
        alias="educacao_especial_tipo_deficiencia",
        schema="br_inep_sinopse_estatistica_educacao_basica",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2007, "end": 2023, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(tipo_classe as string) tipo_classe,
    safe_cast(
        case
            when tipo_deficiencia = 'Altas Habilidades / Superdotação'
            then 'Altas Habilidade/Superdotação'
            else tipo_deficiencia
        end as string
    ) tipo_deficiencia,
    safe_cast(quantidade_matricula as numeric) quantidade_matricula,
from
    `basedosdados-staging.br_inep_sinopse_estatistica_educacao_basica_staging.educacao_especial_tipo_deficiencia`
    as t
