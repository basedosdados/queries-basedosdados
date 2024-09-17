{{
    config(
        alias="quilombolas_indice_envelhecimento_territorio_quilombola",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
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
        replace(
            indice_de_envelhecimento_da_populacao_residente_em_territorios_quilombolas_idosos_60_anos_ou_mais_de_idade_razao_,
            ",",
            "."
        ) as float64
    ) indice_envelhecimento,
    safe_cast(
        idade_mediana_da_populacao_residente_em_territorios_quilombolas_anos_ as int64
    ) idade_mediana,
    safe_cast(
        replace(
            razao_de_sexo_da_populacao_residente_em_territorios_quilombolas_razao_,
            ",",
            "."
        ) as float64
    ) razao_sexo,
    safe_cast(
        replace(
            indice_de_envelhecimento_da_populacao_quilombola_residente_em_territorios_quilombolas_idosos_60_anos_ou_mais_de_idade_razao_,
            ",",
            "."
        ) as float64
    ) indice_envelhecimento_populacao_quilombola,
    safe_cast(
        idade_mediana_da_populacao_quilombola_residente_em_territorios_quilombolas_anos_
        as int64
    ) idade_mediana_populacao_quilombola,
    safe_cast(
        replace(
            razao_de_sexo_da_populacao_quilombola_residente_em_territorios_quilombolas_razao_,
            ",",
            "."
        ) as float64
    ) razao_sexo_quilombola,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.quilombolas_indice_envelhecimento_territorio_quilombola`
    as t
