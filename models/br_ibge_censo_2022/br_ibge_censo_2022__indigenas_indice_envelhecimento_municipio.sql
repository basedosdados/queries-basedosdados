{{ config(alias='indigenas_indice_envelhecimento_municipio',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(cod_ AS STRING) id_municipio,
SAFE_CAST(quesito_de_declaracao_indigena AS STRING) quesito_declaracao_indigena,
SAFE_CAST(REPLACE(indice_de_envelhecimento_da_populacao_indigena_idosos_60_anos_ou_mais_de_idade_razao_, ",", ".") AS FLOAT64) indice_envelhecimento,
SAFE_CAST(idade_mediana_da_populacao_indigena_anos_ AS INT64) idade_mediana,
SAFE_CAST(REPLACE(razao_de_sexo_da_populacao_indigena_razao_, ",", ".") AS FLOAT64) razao_sexo,
FROM basedosdados-staging.br_ibge_censo_2022_staging.indigenas_indice_envelhecimento_municipio AS t

