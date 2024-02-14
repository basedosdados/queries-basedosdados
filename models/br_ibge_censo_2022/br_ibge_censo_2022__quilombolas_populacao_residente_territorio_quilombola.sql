{{
    config(
        alias="quilombolas_populacao_residente_territorio_quilombola",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(cod_ as string) id_territorio_quilombola,
    safe_cast(
        trim(
            regexp_extract(territorio_quilombola_por_unidade_da_federacao, r'([^\(]+)')
        ) as string
    ) territorio_quilombola,
    case
        when
            regexp_contains(
                territorio_quilombola_por_unidade_da_federacao, r'\(\w{2}\)'
            )
        then
            safe_cast(
                trim(
                    regexp_extract(
                        territorio_quilombola_por_unidade_da_federacao, r'\((\w{2})\)'
                    )
                ) as string
            )
        else
            safe_cast(
                trim(
                    split(
                        split(territorio_quilombola_por_unidade_da_federacao, '(')[
                            safe_offset(2)
                        ],
                        ')'
                    )[safe_offset(0)]
                ) as string
            )
    end as sigla_uf,
    safe_cast(
        pessoas_quilombolas_residentes_em_territorios_quilombolas_pessoas_ as int64
    ) pessoas_quilombolas,
    safe_cast(
        pessoas_residentes_em_territorios_quilombolas_pessoas_ as int64
    ) populacao_residente,
from
    basedosdados
    - staging.br_ibge_censo_2022_staging.quilombolas_populacao_residente_territorio_quilombola
    as t
