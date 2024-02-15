{{ config(alias="dicionario", schema="br_ibge_censo_2022") }}

select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor

from `basedosdados-staging.br_ibge_censo_2022_staging.dicionario` as t
