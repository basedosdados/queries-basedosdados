{{
    config(
        alias="microdados_estabelecimentos",
        schema="br_me_rais",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1985, "end": 2023, "interval": 1},
        },
        cluster_by=["sigla_uf"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(quantidade_vinculos_ativos as int64) quantidade_vinculos_ativos,
    safe_cast(quantidade_vinculos_clt as int64) quantidade_vinculos_clt,
    safe_cast(
        quantidade_vinculos_estatutarios as int64
    ) quantidade_vinculos_estatutarios,
    safe_cast(natureza as string) natureza_estabelecimento,
    safe_cast(natureza_juridica as string) natureza_juridica,
    safe_cast(tamanho as string) tamanho_estabelecimento,
    safe_cast(tipo as string) tipo_estabelecimento,
    safe_cast(indicador_cei_vinculado as int64) indicador_cei_vinculado,
    safe_cast(indicador_pat as int64) indicador_pat,
    safe_cast(indicador_simples as string) indicador_simples,
    safe_cast(indicador_rais_negativa as int64) indicador_rais_negativa,
    safe_cast(indicador_atividade_ano as int64) indicador_atividade_ano,
    safe_cast(cnae_1 as string) cnae_1,
    safe_cast(cnae_2 as string) cnae_2,
    safe_cast(cnae_2_subclasse as string) cnae_2_subclasse,
    cast(
        cast(regexp_replace(subsetor_ibge, r'^0+', '') as string) as string
    ) as subsetor_ibge,
    safe_cast(subatividade_ibge as string) subatividade_ibge,
    case
        when length(cep) = 7 then lpad(cep, 8, '0') else cast(cep as string)
    end as cep,
    case
        when bairros_sp = '????????????'
        then null
        else cast(regexp_replace(bairros_sp, r'^0+', '') as string)
    end as bairros_sp,
    cast(regexp_replace(distritos_sp, r'^0+', '') as string) as distritos_sp,
    cast(regexp_replace(bairros_fortaleza, r'^0+', '') as string) as bairros_fortaleza,
    nullif(cast(regexp_replace(bairros_rj, r'^0+', '') as string), '') as bairros_rj,
    cast(
        regexp_replace(regioes_administrativas_df, r'^0+', '') as string
    ) as regioes_administrativas_df
from `basedosdados-staging.br_me_rais_staging.microdados_estabelecimentos` as t
