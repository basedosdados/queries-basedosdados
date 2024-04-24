{{
    config(
        alias="dicionario",
        schema="br_me_comex_stat",
    )
}}
select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(
        case
            when coluna = 'id_urf'
            then lpad(safe_cast(chave as string), 6, '0')
            else safe_cast(chave as string)
        end as string
    ) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from `basedosdados-staging.br_me_comex_stat_staging.dicionario` as t
