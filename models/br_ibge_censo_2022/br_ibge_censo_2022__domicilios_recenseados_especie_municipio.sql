{{ config(alias='domicilios_recenseados_especie_municipio',schema='br_ibge_censo_2022') }}
WITH ibge as(
SELECT
municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'([^\(]+)')) AS STRING) nome_municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(especie AS STRING) especie,
SAFE_CAST(domicilios_recenseados_domicilios_ AS INT64) domicilios,
FROM basedosdados-dev.br_ibge_censo_2022_staging.domicilios_recenseados_especie_municipio AS t)
select t2.cod as id_municipio, ibge.* except(municipio, nome_municipio,sigla_uf) 
from ibge
left join `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
on ibge.municipio = t2.municipio
