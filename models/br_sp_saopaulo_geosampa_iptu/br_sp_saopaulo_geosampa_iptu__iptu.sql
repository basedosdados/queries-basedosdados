{{
    config(
        materialized="table",
        alias="iptu",
        schema="br_sp_saopaulo_geosampa_iptu",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2023, "interval": 1},
        },
    )
}}


select distinct
    safe_cast(ano as int64) ano,
    safe_cast(data_cadastramento as date) data_cadastramento,
    safe_cast(numero_notificacao as string) numero_notificacao,
    safe_cast(numero_contribuinte as string) numero_contribuinte,
    safe_cast(ano_inicio_vida_contribuinte as int64) ano_inicio_vida_contribuinte,
    safe_cast(mes_inicio_vida_contribuinte as int64) mes_inicio_vida_contribuinte,
    safe_cast(logradouro as string) logradouro,
    safe_cast(numero_imovel as int64) numero_imovel,
    safe_cast(numero_condominio as string) numero_condominio,
    safe_cast(complemento as string) complemento,
    safe_cast(bairro as string) bairro,
    case when cep in ('nan', '0') then null else cep end as cep,
    safe_cast(ano_construcao_corrigida as int64) ano_construcao_corrigida,
    safe_cast(fator_obsolescencia as float64) fator_obsolescencia,
    safe_cast(referencia_imovel as string) referencia_imovel,
    safe_cast(finalidade_imovel as string) finalidade_imovel,
    safe_cast(tipo_construcao as string) tipo_construcao,
    safe_cast(tipo_terreno as string) tipo_terreno,
    safe_cast(fracao_ideal as float64) fracao_ideal,
    safe_cast(area_terreno as int64) area_terreno,
    safe_cast(area_construida as int64) area_construida,
    safe_cast(area_ocupada as int64) area_ocupada,
    safe_cast(quantidade_pavimento as int64) quantidade_pavimento,
    safe_cast(quantidade_esquina_imovel as string) quantidade_esquina_imovel,
    safe_cast(testada_imovel as float64) testada_imovel,
    safe_cast(valor_terreno as int64) valor_terreno,
    safe_cast(valor_construcao as int64) valor_construcao,
from `basedosdados-staging.br_sp_saopaulo_geosampa_iptu_staging.iptu` as t
