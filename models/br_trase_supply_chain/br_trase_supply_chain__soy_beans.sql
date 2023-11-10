{{ config(
    alias='soy_beans', 
    schema='br_trase_supply_chain',
    partition_by={
      "field": "year",
      "data_type": "int64",
      "range": {
        "start": 2004,
        "end": 2021,
        "interval": 1}
     }) 
}}


-- padronizar iso3
with inserir_id_iso3 as (
--padronizar colunas que precisam ser tratadas
SELECT 
  *,
  LOWER(TRANSLATE(`COUNTRY OF FIRST IMPORT`, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC'))  AS name_country_first_import,
  LOWER(TRANSLATE(`LOGISTICS HUB`, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) name_logistics_hub,
  SAFE_CAST(SUBSTR(TRASE_GEOCODE, 4,11) AS STRING) municipality_id
  FROM `basedosdados-staging.br_trase_supply_chain_staging.soy_beans`
  
),
iso3 as (
  SELECT *
  FROM inserir_id_iso3
LEFT JOIN (SELECT LOWER(TRANSLATE(nome_ingles, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) as nome_ingles, sigla_pais_iso3 as iso3_country_id FROM `basedosdados.br_bd_diretorios_mundo.pais`) as diretorio_pais
  ON inserir_id_iso3.name_country_first_import = diretorio_pais.nome_ingles
),
iso3_2 as(

SELECT *,
  CASE 
  -- tem valores unknown country e unknown country european union
  -- netherlands antilles -> dissolvida em 2010 para curacao e saint martin https://2009-2017.state.gov/r/pa/ei/bgn/22528.htm
  -- pacific islands (usa) -> não tem no diretório de países
    WHEN name_country_first_import = 'china (mainland)'AND iso3_country_id IS NULL  THEN 'CHN'
    WHEN name_country_first_import = 'netherlands' AND iso3_country_id IS NULL THEN 'NLD'
    WHEN name_country_first_import = 'united kingdom'AND iso3_country_id IS NULL THEN 'GBR'
    WHEN name_country_first_import = 'vietnam' AND iso3_country_id IS NULL THEN 'VNM'
    WHEN name_country_first_import = 'united states'AND iso3_country_id IS NULL THEN 'USA'
    WHEN name_country_first_import = 'south korea' AND iso3_country_id IS NULL THEN 'KOR'
    WHEN name_country_first_import = 'taiwan' AND iso3_country_id IS NULL THEN 'TWN'
    WHEN name_country_first_import = 'iran' AND iso3_country_id IS NULL THEN 'IRN'
    WHEN name_country_first_import = 'venezuela' AND iso3_country_id IS NULL THEN 'VEN'
    WHEN name_country_first_import = 'russian federation' AND iso3_country_id IS NULL THEN 'RUS'
    WHEN name_country_first_import = 'united arab emirates'AND iso3_country_id IS NULL THEN 'ARE'
    WHEN name_country_first_import = 'bolivia' AND iso3_country_id IS NULL THEN 'BOL'
    WHEN name_country_first_import = 'dominican republic' AND iso3_country_id IS NULL THEN 'DOM'
    WHEN name_country_first_import = 'philippines' AND iso3_country_id IS NULL THEN 'PHL'
    WHEN name_country_first_import = 'china (hong kong)' AND iso3_country_id IS NULL THEN 'HKG'
    WHEN name_country_first_import = 'north korea' AND iso3_country_id IS NULL THEN 'PRK'
    WHEN name_country_first_import = 'cayman islands' AND iso3_country_id IS NULL THEN 'CYM'
    WHEN name_country_first_import = 'turks and caicos islands' AND iso3_country_id IS NULL THEN 'TCA'
    WHEN name_country_first_import = 'cape verde' AND iso3_country_id IS NULL THEN 'CPV'
    WHEN name_country_first_import = 'bahamas' AND iso3_country_id IS NULL THEN 'BHS'
    WHEN name_country_first_import = 'gambia' AND iso3_country_id IS NULL THEN 'GMB'
    WHEN name_country_first_import = 'congo' AND iso3_country_id IS NULL THEN 'COG'
    WHEN name_country_first_import = 'sudan' AND iso3_country_id IS NULL THEN 'SDN'
    WHEN name_country_first_import = 'tanzania' AND iso3_country_id IS NULL THEN 'TZA'
    WHEN name_country_first_import = 'virgin islands (uk)' AND iso3_country_id IS NULL THEN 'VGB'
    WHEN name_country_first_import = 'netherlands antilles'AND iso3_country_id IS NULL  THEN 'NLD'
    WHEN name_country_first_import = 'pacific islands (usa)' AND iso3_country_id IS NULL THEN 'HKG'
    WHEN name_country_first_import = 'syria'AND iso3_country_id IS NULL  THEN 'SYR'
    WHEN name_country_first_import = 'congo democratic republic of the'AND iso3_country_id IS NULL  THEN 'COD'
    WHEN name_country_first_import = 'st. vincent and the grenadines' AND iso3_country_id IS NULL THEN 'VCT'
    WHEN name_country_first_import = 'united states virgin islands'AND iso3_country_id IS NULL  THEN 'VIR'
    WHEN name_country_first_import = 'dominica island'AND iso3_country_id IS NULL  THEN 'DMA'
    WHEN name_country_first_import = 'macedonia' AND iso3_country_id IS NULL THEN 'MKD'
    WHEN name_country_first_import = 'marshall islands' AND iso3_country_id IS NULL THEN 'MHL'
    WHEN name_country_first_import = 'st. kitts and nevis' AND iso3_country_id IS NULL THEN 'KNA'
    ELSE iso3_country_id
    END AS iso3_country_id_,
  CASE
    WHEN name_logistics_hub = 'lagoa do itaenga' THEN 'lagoa de itaenga'
    WHEN name_logistics_hub = 'porto naciona' THEN 'porto nacional'
    WHEN name_logistics_hub = 'belo horizont' THEN 'belo horizonte'
    WHEN name_logistics_hub = 'patos de mina' THEN 'patos de minas'
    WHEN name_logistics_hub = 'sao valerio da natividade' THEN 'sao valerio'
    WHEN name_logistics_hub = 'coronel vivid' THEN 'coronel vivida'
    WHEN name_logistics_hub = 'eldorado do s' THEN 'eldorado do sul'
    WHEN name_logistics_hub = 'faxinal dos g' THEN 'faxinal dos guedes'
    ELSE name_logistics_hub
    END AS name_logistics_hub1,
  CASE
    WHEN `COUNTRY OF PRODUCTION` = 'BRAZIL' THEN 'BRA'
    ELSE `COUNTRY OF PRODUCTION`
    END AS country_production_iso3_id,
  -- alguns valores da variável TRASE GEOCODE
  -- não são ids_municipios, o código seguinte corrige isso
  CASE 
    WHEN REGEXP_CONTAINS(municipality_id, r'\D') THEN NULL
    ELSE municipality_id
    END AS municipality_id_production,
  CASE 
    WHEN STATE = 'ACRE' THEN 'AC'
    WHEN STATE = 'ALAGOAS' THEN 'AL'
    WHEN STATE = 'AMAPA' THEN 'AP'
    WHEN STATE = 'AMAZONAS' THEN 'AM'
    WHEN STATE = 'BAHIA' THEN 'BA'
    WHEN STATE = 'CEARA' THEN 'CE'
    WHEN STATE = 'DISTRITO FEDERAL' THEN 'DF'
    WHEN STATE = 'ESPIRITO SANTO' THEN 'ES'
    WHEN STATE = 'GOIAS' THEN 'GO'
    WHEN STATE = 'MARANHAO' THEN 'MA'
    WHEN STATE = 'MATO GROSSO' THEN 'MT'
    WHEN STATE = 'MATO GROSSO DO SUL' THEN 'MS'
    WHEN STATE = 'MINAS GERAIS' THEN 'MG'
    WHEN STATE = 'PARA' THEN 'PA'
    WHEN STATE = 'PARAIBA' THEN 'PB'
    WHEN STATE = 'PARANA' THEN 'PR'
    WHEN STATE = 'PERNAMBUCO' THEN 'PE'
    WHEN STATE = 'PIAUI' THEN 'PI'
    WHEN STATE = 'RIO DE JANEIRO' THEN 'RJ'
    WHEN STATE = 'RIO GRANDE DO NORTE' THEN 'RN'
    WHEN STATE = 'RIO GRANDE DO SUL' THEN 'RS'
    WHEN STATE = 'RONDONIA' THEN 'RO'
    WHEN STATE = 'RORAIMA' THEN 'RR'
    WHEN STATE = 'SANTA CATARINA' THEN 'SC'
    WHEN STATE = 'SAO PAULO' THEN 'SP'
    WHEN STATE = 'SERGIPE' THEN 'SE'
    WHEN STATE = 'TOCANTINS' THEN 'TO'
    ELSE ' '
    END AS state_production,
FROM iso3),
--adicionar id_municipio do logistics hub
add_logistics as (
SELECT *
from iso3_2
LEFT JOIN (
  SELECT 
    LOWER(TRANSLATE(nome, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) as nome, 
    id_municipio as municipality_id_logistics_hub 
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`

  ) as diretorio
ON iso3_2.name_logistics_hub1 = diretorio.nome
AND diretorio.nome  NOT IN (
  'santana', 'nova olimpia', 'agua boa', 'canarana', 'santa maria', 'sao simao', 'cafelandia', 'presidente kennedy', 'redencao', 'alto alegre',
  'boa vista', 'palmas', 'candeias', 'santa luzia', 'lagoa santa', 'bom jesus', 'guaira', 'jardinopolis', 'sertaozinho',
  'pinhao', 'planalto', 'rio negro', 'santa helena', 'terra roxa', 'turvo', 'marau', 'triunfo', 'soledade', 'sao gabriel', 'buritis',
  'capanema', 'bonito', 'alvorada', 'colinas', 'riachao', 'santa filomena', 'bocaina', 'morrinhos', 'cascavel', 'jardim', 'campo grande','palmeira',
  'pedra preta', 'floresta', 'sao joao', 'itambe', 'campo alegre', 'toledo', 'eldorado', 'tapejara', 'bandeirantes', 'nova aurora', 'irati', 'general carneiro')
)

SELECT
SAFE_CAST(YEAR AS INT64) year,
SAFE_CAST(BIOME AS STRING) biome,
SAFE_CAST(country_production_iso3_id AS STRING) country_production_iso3_id,
SAFE_CAST(state_production AS STRING) state_production,
SAFE_CAST(LOWER(`MUNICIPALITY OF PRODUCTION`) AS STRING) municipality_name_production,
SAFE_CAST(REPLACE(municipality_id, 'XXXXXXX', '') AS STRING) municipality_id_production,
SAFE_CAST(name_logistics_hub AS STRING) municipality_name_logistics_hub,
SAFE_CAST(municipality_id_logistics_hub AS STRING) municipality_id_logistics_hub,
SAFE_CAST(REPLACE(`PORT OF EXPORT`, 'UNKNOWN', '') AS STRING) export_port,
SAFE_CAST(REPLACE(EXPORTER, 'UNKNOWN', '') AS STRING) exporter_name,
SAFE_CAST(REPLACE(`EXPORTER GROUP`, 'UNKNOWN', '') AS STRING) exporter_group,
SAFE_CAST(REPLACE(IMPORTER, 'UNKNOWN', '') AS STRING) importer_name,
SAFE_CAST(REPLACE(`IMPORTER GROUP`, 'UNKNOWN', '') AS STRING) importer_group,
SAFE_CAST(iso3_country_id_ AS STRING) country_first_import_iso3_id,
SAFE_CAST(`COUNTRY OF FIRST IMPORT` AS STRING) country_first_import_name,
SAFE_CAST(`ECONOMIC BLOC` AS STRING) economic_bloc_first_import_name,
SAFE_CAST(FOB_USD AS FLOAT64) fob_usd,
SAFE_CAST(SOY_EQUIVALENT_TONNES AS FLOAT64) soy_total_export,
SAFE_CAST(LAND_USE_HA AS FLOAT64) land_use,
SAFE_CAST(`Soy deforestation exposure` AS STRING) soy_deforestation_exposure,
SAFE_CAST(ZERO_DEFORESTATION_BRAZIL_SOY AS STRING) zero_deforestation_commitments,
SAFE_CAST(CO2_GROSS_EMISSIONS_SOY_DEFORESTATION_5_YEAR_TOTAL_EXPOSURE AS FLOAT64) co2_gross_emissions_deforestation_5,
SAFE_CAST(CO2_NET_EMISSIONS_SOY_DEFORESTATION_5_YEAR_TOTAL_EXPOSURE AS FLOAT64) co2_net_emissions_deforestation_5,
SAFE_CAST(`Soy deforestation risk` AS FLOAT64) soy_risk,
SAFE_CAST(TYPE AS STRING) type,
FROM add_logistics

