{{ config(alias='quilombolas_populacao_residente_municipio',schema='br_ibge_censo_2022') }}
WITH ibge as (
SELECT
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'([^\(]+)')) AS STRING) nome_municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(localizacao_do_domicilio AS STRING) territorios_quilombolas,
SAFE_CAST(pessoas_quilombolas_pessoas_ AS INT64) fora_territorios_quilombolas,
SAFE_CAST(populacao_residente_pessoas_ AS INT64) fora_territorios_quilombolas,
FROM basedosdados-staging.br_ibge_censo_2022_staging.quilombolas_populacao_residente_municipio AS t)
select t2.id_municipio, ibge.* except(nome_municipio,sigla_uf) 
from ibge
left join `basedosdados.br_bd_diretorios_brasil.municipio` t2
on ibge.nome_municipio = t2.nome and ibge.sigla_uf = t2.sigla_uf