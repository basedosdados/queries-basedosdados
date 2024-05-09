{{
    config(
        alias="indigenas_indice_envelhecimento_terras_indigenas",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
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
    safe_cast(quesito_de_declaracao_indigena as string) quesito_declaracao_indigena,
    safe_cast(
        replace(
            indice_de_envelhecimento_da_populacao_residente_em_terras_indigenas_idosos_60_anos_ou_mais_de_idade_razao_,
            ",",
            "."
        ) as float64
    ) indice_envelhecimento,
    safe_cast(
        idade_mediana_da_populacao_residente_em_terras_indigenas_anos_ as int64
    ) idade_mediana,
    safe_cast(
        replace(
            razao_de_sexo_da_populacao_residente_em_terras_indigenas_razao_, ",", "."
        ) as float64
    ) razao_sexo,
    safe_cast(
        replace(
            indice_de_envelhecimento_da_populacao_indigena_residente_em_terras_indigenas_idosos_60_anos_ou_mais_de_idade_razao_,
            ",",
            "."
        ) as float64
    ) indice_envelhecimento_populacao_indigena,
    safe_cast(
        idade_mediana_da_populacao_indigena_residente_em_terras_indigenas_anos_ as int64
    ) idade_mediana_populacao_indigena,
    safe_cast(
        replace(
            razao_de_sexo_da_populacao_indigena_residente_em_terras_indigenas_razao_,
            ",",
            "."
        ) as float64
    ) razao_sexo_populacao_indigena,
from
    `basedosdados-dev.br_ibge_censo_2022_staging.indigenas_indice_envelhecimento_terras_indigenas`
    as t
