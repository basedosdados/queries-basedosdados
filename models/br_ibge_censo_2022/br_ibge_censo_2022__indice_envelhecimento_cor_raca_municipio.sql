{{ config(alias='indice_envelhecimento_cor_raca_municipio',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(cod_ AS STRING) id_municipio,
SAFE_CAST(cor_ou_raca AS STRING) cor_raca,
SAFE_CAST(REPLACE(indice_de_envelhecimento_idosos_60_anos_ou_mais_de_idade_razao_, ",", ".") AS FLOAT64) indice_envelhecimento,
SAFE_CAST(idade_mediana_anos_ AS INT64) idade_mediana,
SAFE_CAST(REPLACE(razao_de_sexo_razao_, ",", ".") AS FLOAT64) razao_sexo,
FROM basedosdados-staging.br_ibge_censo_2022_staging.indice_envelhecimento_cor_raca_municipio AS t

