{{ config(alias='domicilios_recenseados_especie_municipio',schema='br_ibge_censo_2022') }}
WITH ibge as (
SELECT
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'([^\(]+)')) AS STRING) nome_municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(especie AS STRING) especie,
SAFE_CAST(domicilios_recenseados_domicilios_ AS INT64) domicilios_recenseados,
FROM basedosdados-staging.br_ibge_censo_2022_staging.domicilios_recenseados_especie_municipio AS t)
select t2.id_municipio, ibge.* except(nome_municipio,sigla_uf) 
from ibge
left join `basedosdados.br_bd_diretorios_brasil.municipio` t2
on ibge.nome_municipio = t2.nome and ibge.sigla_uf = t2.sigla_uf


