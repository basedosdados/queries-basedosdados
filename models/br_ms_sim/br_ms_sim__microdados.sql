{{
    config(
        alias="microdados",
        schema="br_ms_sim",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1996, "end": 2022, "interval": 1},
        },
        cluster_by="sigla_uf",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(sequencial_obito as string) sequencial_obito,
    safe_cast(tipo_obito as string) tipo_obito,
    safe_cast(causa_basica as string) causa_basica,
    safe_cast(data_obito as date) data_obito,
    safe_cast(hora_obito as time) hora_obito,
    safe_cast(naturalidade as string) naturalidade,
    safe_cast(data_nascimento as date) data_nascimento,
    safe_cast(idade as float64) idade,
    safe_cast(sexo as string) sexo,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(estado_civil as string) estado_civil,
    safe_cast(escolaridade as string) escolaridade,
    safe_cast(ocupacao as string) ocupacao,
    safe_cast(codigo_bairro_residencia as string) codigo_bairro_residencia,
    safe_cast(id_municipio_residencia as string) id_municipio_residencia,
    safe_cast(local_ocorrencia as string) local_ocorrencia,
    safe_cast(codigo_bairro_ocorrencia as string) codigo_bairro_ocorrencia,
    safe_cast(id_municipio_ocorrencia as string) id_municipio_ocorrencia,
    safe_cast(idade_mae as int64) idade_mae,
    safe_cast(escolaridade_mae as string) escolaridade_mae,
    safe_cast(ocupacao_mae as string) ocupacao_mae,
    safe_cast(quantidade_filhos_vivos as int64) quantidade_filhos_vivos,
    safe_cast(quantidade_filhos_mortos as int64) quantidade_filhos_mortos,
    safe_cast(gravidez as string) gravidez,
    safe_cast(gestacao as string) gestacao,
    safe_cast(parto as string) parto,
    safe_cast(obito_parto as string) obito_parto,
    safe_cast(morte_parto as string) morte_parto,
    safe_cast(peso as int64) peso,
    safe_cast(obito_gravidez as string) obito_gravidez,
    safe_cast(obito_puerperio as string) obito_puerperio,
    safe_cast(assistencia_medica as string) assistencia_medica,
    safe_cast(exame as string) exame,
    safe_cast(cirurgia as string) cirurgia,
    safe_cast(necropsia as string) necropsia,
    safe_cast(linha_a as string) linha_a,
    safe_cast(linha_b as string) linha_b,
    safe_cast(linha_c as string) linha_c,
    safe_cast(linha_d as string) linha_d,
    safe_cast(linha_ii as string) linha_ii,
    safe_cast(circunstancia_obito as string) circunstancia_obito,
    safe_cast(acidente_trabalho as string) acidente_trabalho,
    safe_cast(fonte as string) fonte,
    safe_cast(codigo_estabelecimento as string) codigo_estabelecimento,
    safe_cast(atestante as string) atestante,
    safe_cast(data_atestado as date) data_atestado,
    safe_cast(tipo_pos as string) tipo_pos,
    safe_cast(data_investigacao as date) data_investigacao,
    safe_cast(causa_basica_original as string) causa_basica_original,
    safe_cast(data_cadastro as date) data_cadastro,
    safe_cast(fonte_investigacao as string) fonte_investigacao,
    safe_cast(data_recebimento as date) data_recebimento,
    safe_cast(causa_basica_pre as string) causa_basica_pre,
    safe_cast(tipo_obito_ocorrencia as string) tipo_obito_ocorrencia,
    safe_cast(tipo_morte_ocorrencia as string) tipo_morte_ocorrencia,
    safe_cast(data_cadastro_informacao as date) data_cadastro_informacao,
    safe_cast(data_cadastro_investigacao as date) data_cadastro_investigacao,
    safe_cast(id_municipio_svo_iml as string) id_municipio_svo_iml,
    safe_cast(data_recebimento_original as date) data_recebimento_original,
    safe_cast(data_recebimento_original_a as date) data_recebimento_original_a,
    safe_cast(causa_materna as string) causa_materna,
    safe_cast(status_do_epidem as string) status_do_epidem,
    safe_cast(status_do_nova as string) status_do_nova,
    safe_cast(serie_escolar_falecido as int64) serie_escolar_falecido,
    safe_cast(serie_escolar_mae as int64) serie_escolar_mae,
    safe_cast(escolaridade_2010 as string) escolaridade_2010,
    safe_cast(escolaridade_mae_2010 as string) escolaridade_mae_2010,
    safe_cast(escolaridade_falecido_2010_agr as string) escolaridade_falecido_2010_agr,
    safe_cast(escolaridade_mae_2010_agr as string) escolaridade_mae_2010_agr,
    safe_cast(semanas_gestacao as int64) semanas_gestacao,
    safe_cast(diferenca_data as int64) diferenca_data,
    safe_cast(data_conclusao_investigacao as date) data_conclusao_investigacao,
    safe_cast(data_conclusao_caso as date) data_conclusao_caso,
    safe_cast(numero_dias_obito_investigacao as int64) numero_dias_obito_investigacao,
    safe_cast(id_municipio_naturalidade as string) id_municipio_naturalidade,
    safe_cast(descricao_estabelecimento as string) descricao_estabelecimento,
    safe_cast(crm as string) crm,
    safe_cast(numero_lote as string) numero_lote,
    safe_cast(status_codificadora as string) status_codificadora,
    safe_cast(codificado as string) codificado,
    safe_cast(versao_sistema as string) versao_sistema,
    safe_cast(versao_scb as string) versao_scb,
    safe_cast(atestado as string) atestado,
    safe_cast(numero_dias_obito_ficha as int64) numero_dias_obito_ficha,
    safe_cast(fontes as string) fontes,
    safe_cast(tipo_resgate_informacao as string) tipo_resgate_informacao,
    safe_cast(tipo_nivel_investigador as string) tipo_nivel_investigador,
    safe_cast(numero_dias_informacao as int64) numero_dias_informacao,
    safe_cast(fontes_informacao as string) fontes_informacao,
    safe_cast(alt_causa as string) alt_causa
from {{ set_datalake_project("br_ms_sim_staging.microdados") }} as t
