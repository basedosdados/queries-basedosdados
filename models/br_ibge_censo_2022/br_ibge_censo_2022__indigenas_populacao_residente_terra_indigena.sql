{{
    config(
        alias="indigenas_populacao_residente_terra_indigena",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(cod_ as string) id_terra_indigena,
    safe_cast(
        trim(
            regexp_extract(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')
        ) as string
    ) terra_indigena,
    case
        when regexp_contains(terra_indigena_por_unidade_da_federacao, r'\(\w{2}\)')
        then
            safe_cast(
                trim(
                    regexp_extract(
                        terra_indigena_por_unidade_da_federacao, r'\((\w{2})\)'
                    )
                ) as string
            )
        else
            safe_cast(
                trim(
                    split(
                        split(terra_indigena_por_unidade_da_federacao, '(')[
                            safe_offset(2)
                        ],
                        ')'
                    )[safe_offset(0)]
                ) as string
            )
    end as sigla_uf,
    safe_cast(
        pessoas_residentes_em_terras_indigenas_pessoas_ as int64
    ) populacao_residente,
    safe_cast(
        pessoas_indigenas_residentes_em_terras_indigenas_pessoas_ as int64
    ) pessoas_indigenas,
    safe_cast(quesito_de_declaracao_indigena as string) quesito_declaracao_indigena,
from
    basedosdados
    - staging.br_ibge_censo_2022_staging.indigenas_populacao_residente_terra_indigena
    as t
