{{
    config(
        schema="br_mec_sisu",
        alias="microdados",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2017, "end": 2024, "interval": 1},
        },
        cluster_by=["ano", "sigla_uf_candidato"],
        labels={"tema": "educacao"},
    )
}}

select
    safe_cast(ano as int64) as ano,
    safe_cast(edicao as string) as edicao,
    safe_cast(etapa as string) as etapa,
    safe_cast(sigla_uf_ies as string) as sigla_uf_ies,
    safe_cast(id_ies as string) as id_ies,
    safe_cast(sigla_ies as string) as sigla_ies,
    safe_cast(sigla_uf_campus as string) as sigla_uf_campus,
    safe_cast(d1.id_municipio as string) as id_municipio_campus,
    safe_cast(id_campus as string) as id_campus,
    safe_cast(campus as string) as campus,
    safe_cast(id_curso as string) as id_curso,
    case
        when turno = 'Integral'
        then '1'
        when turno = 'Matutino'
        then '2'
        when turno = 'Vespertino'
        then '3'
        when turno = 'Noturno'
        then '4'
        when turno = 'EaD'
        then '5'
    end as turno,
    case
        when periodicidade = 'Trimestral'
        then '3'
        when periodicidade = 'Quadrimestral'
        then '4'
        when periodicidade = 'Semestral'
        then '6'
        when periodicidade = 'Anual'
        then '12'
    end as periodicidade,
    safe_cast(tipo_cota as string) as tipo_cota,
    safe_cast(ds_modalidade_concorrencia as string) as modalidade_concorrencia,
    safe_cast(quantidade_vagas_concorrencia as int64) as quantidade_vagas_concorrencia,
    safe_cast(percentual_bonus as float64) as percentual_bonus,
    safe_cast(peso_l as float64) as peso_l,
    safe_cast(peso_ch as float64) as peso_ch,
    safe_cast(peso_cn as float64) as peso_cn,
    safe_cast(peso_m as float64) as peso_m,
    safe_cast(peso_r as float64) as peso_r,
    safe_cast(nota_minima_l as float64) as nota_minima_l,
    safe_cast(nota_minima_ch as float64) as nota_minima_ch,
    safe_cast(nota_minima_cn as float64) as nota_minima_cn,
    safe_cast(nota_minima_m as float64) as nota_minima_m,
    safe_cast(nota_minima_r as float64) as nota_minima_r,
    safe_cast(media_minima as float64) as media_minima,
    safe_cast(cpf as string) as cpf,
    safe_cast(inscricao_enem as string) as inscricao_enem,
    safe_cast(candidato as string) as candidato,
    safe_cast(sexo as string) as sexo,
    case
        when
            (
                (length(data_nascimento) = 8)
                and (cast(substr(data_nascimento, 1, 2) as int64) > 30)
            )
        then concat('19', data_nascimento)
        when
            (
                (length(data_nascimento) = 8)
                and (cast(substr(data_nascimento, 1, 2) as int64) < 30)
            )
        then concat('20', data_nascimento)
        else data_nascimento
    end as data_nascimento,
    safe_cast(sigla_uf_candidato as string) as sigla_uf_candidato,
    safe_cast(d2.id_municipio as string) as id_municipio_candidato,
    safe_cast(opcao as string) as opcao,
    safe_cast(nota_l as float64) as nota_l,
    safe_cast(nota_ch as float64) as nota_ch,
    safe_cast(nota_cn as float64) as nota_cn,
    safe_cast(nota_m as float64) as nota_m,
    safe_cast(nota_r as float64) as nota_r,
    safe_cast(nota_l_peso as float64) as nota_l_peso,
    safe_cast(nota_ch_peso as float64) as nota_ch_peso,
    safe_cast(nota_cn_peso as float64) as nota_cn_peso,
    safe_cast(nota_m_peso as float64) as nota_m_peso,
    safe_cast(nota_r_peso as float64) as nota_r_peso,
    safe_cast(nota_candidato as float64) as nota_candidato,
    safe_cast(nota_corte as float64) as nota_corte,
    safe_cast(classificacao as int64) as classificacao,
    safe_cast(
        (
            case
                when status_aprovado = 'N'
                then false
                when status_aprovado = 'S'
                then true
            end
        ) as bool
    ) as status_aprovado,
    case
        when status_matricula = 'CANCELADA'
        then '1'
        when status_matricula = 'DOCUMENTACAO REJEITADA'
        then '2'
        when status_matricula = 'DOCUMENTAÇÃO REJEITADA'
        then '2'
        when status_matricula = 'EFETIVADA'
        then '3'
        when status_matricula = 'NÃO COMPARECEU'
        then '4'
        when status_matricula = 'NÃO CONVOCADO'
        then '5'
        when status_matricula = 'PENDENTE'
        then '6'
        when status_matricula = 'SUBSTITUIDA - FORA DO PRAZO'
        then '7'
        when status_matricula = 'SUBSTITUIDA - MATRICULA FORA DO PRAZO'
        then '7'
        when status_matricula = 'SUBSTITUIDA - MESMA IES'
        then '8'
        when status_matricula = 'SUBSTITUIDA - OUTRA IES'
        then '9'
        when status_matricula = 'SUBSTITUÍDA MESMA IES'
        then '8'
        when status_matricula = 'SUBSTITUÍDA OUTRA IES'
        then '9'
    end as status_matricula
from `basedosdados-staging.br_mec_sisu_staging.microdados` s
left join
    `basedosdados-staging.br_bd_diretorios_brasil.municipio` d1
    on lower(s.nome_municipio_campus) = lower(d1.nome)
left join
    `basedosdados-staging.br_bd_diretorios_brasil.municipio` d2
    on lower(s.nome_municipio_candidato) = lower(d2.nome)
