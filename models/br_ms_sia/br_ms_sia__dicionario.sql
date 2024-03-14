{{
    config(
        alias="dicionario",
        schema="br_ms_sia",
        materialized="table",
    )
}}


with
    dict as (
        select
            id_tabela,
            coluna,
            cobertura_temporal,
            valor,
            case
                when id_tabela = 'producao_ambulatorial' and coluna = 'tipo_unidade'
                then lpad(chave, 2, '0')
                when
                    id_tabela = 'producao_ambulatorial'
                    and coluna = 'tipo_financiamento_producao'
                then lpad(chave, 2, '0')
                when
                    id_tabela = 'producao_ambulatorial'
                    and coluna = 'carater_atendimento'
                then lpad(chave, 2, '0')
                when
                    id_tabela = 'producao_ambulatorial' and coluna = 'raca_cor_paciente'
                then lpad(chave, 2, '0')
                when
                    id_tabela = 'producao_ambulatorial'
                    and coluna = 'motivo_saida_paciente'
                then lpad(chave, 2, '0')
                when
                    id_tabela = 'producao_ambulatorial'
                    and coluna = 'subtipo_financiamento_producao'
                then lpad(chave, 6, '0')
                else chave
            end as chave2
        from basedosdados - staging.br_ms_sia_staging.dicionario
    )

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(coluna as string) nome_coluna,
    safe_cast(chave2 as string) chave,
    safe_cast(replace(cobertura_temporal, '-1', '(1)') as string) cobertura_temporal,
    safe_cast(valor as string) valor
from dict
