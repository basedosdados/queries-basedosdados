{{ config(alias="dicionario", schema="br_stf_corte_aberta") }}

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    initcap(chave) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    initcap(valor) valor
from `basedosdados-staging.br_stf_corte_aberta_staging.dicionario` as t
