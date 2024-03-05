{{
    config(
        alias="indigenas_indice_envelhecimento_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(quesito_de_declaracao_indigena as string) quesito_declaracao_indigena,
    safe_cast(
        replace(
            indice_de_envelhecimento_da_populacao_indigena_idosos_60_anos_ou_mais_de_idade_razao_,
            ",",
            "."
        ) as float64
    ) indice_envelhecimento,
    safe_cast(idade_mediana_da_populacao_indigena_anos_ as int64) idade_mediana,
    safe_cast(
        replace(razao_de_sexo_da_populacao_indigena_razao_, ",", ".") as float64
    ) razao_sexo,
from
    `basedosdados-staging.br_ibge_censo_2022_staging.indigenas_indice_envelhecimento_municipio` t
