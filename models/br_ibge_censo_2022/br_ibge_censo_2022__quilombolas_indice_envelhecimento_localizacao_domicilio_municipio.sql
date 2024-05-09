{{
    config(
        alias="quilombolas_indice_envelhecimento_localizacao_domicilio_municipio",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(cod_ as string) id_municipio,
    safe_cast(localizacao_do_domicilio as string) localizacao_domicilio,
    safe_cast(
        replace(
            indice_de_envelhecimento_da_populacao_quilombola_idosos_60_anos_ou_mais_razao_,
            ",",
            "."
        ) as float64
    ) indice_envelhecimento,
    safe_cast(idade_mediana_da_populacao_quilombola_anos_ as int64) idade_mediana,
    safe_cast(
        replace(razao_de_sexo_da_populacao_quilombola_razao_, ",", ".") as float64
    ) razao_sexo,
from
    `basedosdados-dev.br_ibge_censo_2022_staging.quilombolas_indice_envelhecimento_localizacao_domicilio_municipio`
    as t
