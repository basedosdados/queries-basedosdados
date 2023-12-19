{{ config(alias='quilombolas_domicilios_moradores_municipio',schema='br_ibge_censo_2022') }}
WITH ibge as (
SELECT
municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'([^\(]+)')) AS STRING) nome_municipio,
SAFE_CAST(TRIM(REGEXP_EXTRACT(municipio, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(domicilios_particulares_permanentes_ocupados_domicilios_ AS INT64) domicilios,
SAFE_CAST(moradores_em_domicilios_particulares_permanentes_ocupados_pessoas_ AS INT64) moradores_domicilios,
SAFE_CAST(moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_pessoas_ AS INT64) moradores_quilombolas_domicilios,
SAFE_CAST(REPLACE(media_de_moradores_em_domicilios_particulares_permanentes_ocupados_pessoas_, ",", ".") AS FLOAT64) media_moradres_domicilios,
SAFE_CAST(REPLACE(media_de_moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_, ",", ".") AS FLOAT64) media_moradores_quilombolas_domicilios,
SAFE_CAST(localizacao_do_domicilio AS STRING) localizacao_domicilio,
FROM basedosdados-dev.br_ibge_censo_2022_staging.quilombolas_domicilios_moradores_municipio AS t)
select t2.cod as id_municipio, ibge.* except(municipio, nome_municipio,sigla_uf) 
from ibge
left join `basedosdados-dev.br_ibge_censo_2022_staging.auxiliary_table` t2
on ibge.municipio = t2.municipio

