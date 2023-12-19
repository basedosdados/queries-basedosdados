{{ config(alias='indigenas_populacao_residente_municipio',schema='br_ibge_censo_2022') }}
WITH ibge as (
SELECT
municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'([^\(]+)')) AS STRING) nome_municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(quesito_de_declaracao_indigena AS STRING) quesito_declaracao_indigena,
SAFE_CAST(localizacao_do_domicilio AS STRING) localizacao_domicilio,
SAFE_CAST(pessoas_indigenas_pessoas_ AS INT64) pessoas_indigenas,
SAFE_CAST(populacao_residente_pessoas_ AS INT64) populacao_residente,
FROM basedosdados-dev.br_ibge_censo_2022_staging.indigenas_populacao_residente_municipio AS t)
select t2.cod as id_municipio, ibge.* except(municipio, nome_municipio,sigla_uf) 
from ibge
left join `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
on ibge.municipio = t2.municipio