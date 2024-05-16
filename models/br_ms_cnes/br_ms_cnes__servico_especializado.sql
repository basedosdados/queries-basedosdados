{{
    config(
        schema="br_ms_cnes",
        alias="servico_especializado",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2005, "end": 2024, "interval": 1},
        },
    )
}}
with
    raw_cnes_servico_especializado as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from `basedosdados-staging.br_ms_cnes_staging.servico_especializado`
        where cnes is not null
    ),
    raw_cnes_servico_especializado_without_duplicates as (
        -- 2. distinct nas linhas
        select distinct * from raw_cnes_servico_especializado
    ),
    cnes_add_muni as (
        -- 3. Adicionar id_municipio e sigla_uf
        select *
        from raw_cnes_servico_especializado_without_duplicates
        left join
            (
                select id_municipio, id_municipio_6,
                from `basedosdados.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_servico_especializado_without_duplicates.codufmun
            = mun.id_municipio_6
    )

select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sigla_uf as string) as sigla_uf,
    safe_cast(id_municipio as string) as id_municipio,
    safe_cast(cnes as string) as id_estabelecimento_cnes,
    ltrim(safe_cast(serv_esp as string), '0') as tipo_servico_especializado,
    concat(
        ltrim(safe_cast(serv_esp as string), '0'), class_sr
    ) as subtipo_servico_especializado,
    ltrim(safe_cast(srvunico as string), '0') as tipo_servico_especializado_unico,
    safe_cast(caracter as string) as tipo_caracterizacao,
    safe_cast(amb_nsus as int64) as indicador_servico_ambulatorial_sus,
    safe_cast(amb_sus as int64) as indicador_servico_nao_sus,
    safe_cast(hosp_nsus as int64) as indicador_servico_hospitalar_nao_sus,
    safe_cast(hosp_sus as int64) as indicador_servico_hospitalar_sus,
    safe_cast(contsrvu as int64) as indicador_servico_especializado_unico,
    safe_cast(cnesterc as int64) as quantidade_nacional_estabelecimento_saude_terceiro
from cnes_add_muni as t

{% if is_incremental() %}
    where

        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
