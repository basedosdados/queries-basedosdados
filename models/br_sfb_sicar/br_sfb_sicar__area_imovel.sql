{{
    config(
        alias="area_imovel",
        schema="br_sfb_sicar",
        materialized="incremental",
        partition_by={
            "field": "data_atualizacao_car",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["sigla_uf"],
    )
}}

with
    municipios_car as (
        select distinct
            safe_cast(cod_estado as string) sigla_uf,
            safe_cast(municipio as string) municipio_nome,

        from `basedosdados-staging.br_sfb_sicar_staging.area_imovel` as t
    ),

    muncipios_car_diretorio as (
        select sd.*, m.id_municipio as id_municipio
        from municipios_car sd
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as m
            on lower(
                regexp_replace(normalize(sd.municipio_nome, nfd), r"[^a-zA-Z0-9\s]", "")
            )
            = lower(regexp_replace(normalize(m.nome, nfd), r"[^a-zA-Z0-9\s]", ""))
            and sd.sigla_uf = m.sigla_uf
    ),
    correcao_manual_falhas as (
        select
            sigla_uf,
            municipio_nome,
            case
                when sigla_uf = 'PE' and municipio_nome = 'Iguaracy'
                then '2606903'
                when sigla_uf = 'RN' and municipio_nome = 'Januario Cicco'
                then '2405306'
                when sigla_uf = 'RN' and municipio_nome = "Olho d'Agua do Borges"
                then '2408409'
                when sigla_uf = 'PA' and municipio_nome = "Santa Izabel do Para"
                then '1506500'
                when sigla_uf = 'SP' and municipio_nome = "Florinea"
                then '3516101'
                when sigla_uf = 'SP' and municipio_nome = "Sao Luiz do Paraitinga"
                then '3550001'
                when sigla_uf = 'SP' and municipio_nome = "Biritiba Mirim"
                then '3506607'
                when sigla_uf = 'MT' and municipio_nome = "Santo Antonio de Leverger"
                then '5107800'
                when sigla_uf = 'MT' and municipio_nome = "Poxoreu"
                then '5107008'
                when sigla_uf = 'BA' and municipio_nome = "Muquem do Sao Francisco"
                then '2922250'
                when sigla_uf = 'MG' and municipio_nome = "Passa Vinte"
                then '3147808'
                when sigla_uf = 'SE' and municipio_nome = "Amparo do Sao Francisco"
                then '2800100'
                when sigla_uf = 'BA' and municipio_nome = "Santa Terezinha"
                then '2928505'
                when sigla_uf = 'TO' and municipio_nome = "Tabocao"
                then '1708254'
                when sigla_uf = 'MG' and municipio_nome = "Dona Euzebia"
                then '3122900'
                when sigla_uf = 'MG' and municipio_nome = "Sao Tome das Letras"
                then '3165206'
                when sigla_uf = 'SC' and municipio_nome = "Grao-Para"
                then '4206108'
                when sigla_uf = 'CE' and municipio_nome = "Itapaje"
                then '2306306'
                else id_municipio
            end as id_municipio
        from muncipios_car_diretorio
    ),

    final_table as (
        select
            safe_cast(data_extracao as date) data_extracao,
            safe_cast(data_atualizacao_car as date) data_atualizacao_car,
            safe_cast(cod_estado as string) sigla_uf,
            safe_cast(t2.id_municipio as string) id_municipio,
            safe_cast(cod_imovel as string) id_imovel,
            safe_cast(mod_fiscal as string) modulos_fiscais,
            safe_cast(num_area as float64) area,
            safe_cast(ind_status as string) status,
            safe_cast(ind_tipo as string) tipo,
            safe_cast(des_condic as string) condicao,
            safe_cast(
                safe.st_geogfromtext(geometry, make_valid => true) as geography
            ) geometria,
        from `basedosdados-staging.br_sfb_sicar_staging.area_imovel` as car
        left join
            correcao_manual_falhas as t2
            on car.sigla_uf = t2.sigla_uf
            and car.municipio = t2.municipio_nome

    )

select *
from final_table

{% if is_incremental() %}
    where data_extracao > (select max(data_extracao) from {{ this }})
{% endif %}
