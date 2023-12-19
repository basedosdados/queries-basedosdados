{{ config(alias='indice_envelhecimento_municipio',schema='br_ibge_censo_2022') }}
WITH ibge as (
SELECT
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'([^\(]+)')) AS STRING) nome_municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(REPLACE(indice_de_envelhecimento_razao_, ",", ".") AS FLOAT64) indice_envelhecimento,
SAFE_CAST(REPLACE(idade_mediana_anos_,  ",", ".") AS FLOAT64) idade_mediana,
SAFE_CAST(REPLACE(razao_de_sexo_razao_, ",", ".") AS FLOAT64) razao_sexo,
FROM basedosdados-staging.br_ibge_censo_2022_staging.indice_envelhecimento_municipio AS t)
select t2.id_municipio, ibge.* except(nome_municipio,sigla_uf) 
from ibge
left join `basedosdados.br_bd_diretorios_brasil.municipio` t2
on ibge.nome_municipio = t2.nome and ibge.sigla_uf = t2.sigla_uf