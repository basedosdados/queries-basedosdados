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
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter                     ON {{this}}                     GRANT TO ("allUsers")                     FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter        ON  {{this}}                     GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")                     FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)',
        ],
    )
}}
with
    raw_cnes_servico_especializado as (
        -- 1. Retirar linhas com id_estabelecimento_cnes nulo
        select *
        from `basedosdados-dev.br_ms_cnes_staging.servico_especializado`
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
                from `basedosdados-dev.br_bd_diretorios_brasil.municipio`
            ) as mun
            on raw_cnes_servico_especializado_without_duplicates.codufmun
            = mun.id_municipio_6
    )

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(cnes as string) id_estabelecimento_cnes,
    safe_cast(serv_esp as string) tipo_servico_especializado,
    safe_cast(class_sr as string) tipo_classificacao,
    safe_cast(concat(serv_esp, class_sr) as string) tipo_classificacao_bd,
    safe_cast(srvunico as string) tipo_servico_especializado_unico,
    safe_cast(caracter as string) tipo_caracterizacao,
    safe_cast(amb_nsus as int64) indicador_servico_ambulatorial_sus,
    safe_cast(amb_sus as int64) indicador_servico_nao_sus,
    safe_cast(hosp_nsus as int64) indicador_servico_hospitalar_nao_sus,
    safe_cast(hosp_sus as int64) indicador_servico_hospitalar_sus,
    safe_cast(contsrvu as int64) indicador_servico_especializado_unico,
    safe_cast(cnesterc as int64) quantidade_nacional_estabelecimento_saude_terceiro
from cnes_add_muni as t
{% if is_incremental() %}
    where

        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
