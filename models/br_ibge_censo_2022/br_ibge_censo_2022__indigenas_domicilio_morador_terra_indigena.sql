{{ config(alias='indigenas_domicilio_morador_terra_indigena',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(cod_ AS STRING) id_terra_indigena,
SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')) AS STRING) terra_indigena,
  CASE
    WHEN REGEXP_CONTAINS(terra_indigena_por_unidade_da_federacao, r'\(\w{2}\)') THEN
      SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'\((\w{2})\)')) AS STRING)
    ELSE
      SAFE_CAST(TRIM(SPLIT(SPLIT(terra_indigena_por_unidade_da_federacao, '(')[SAFE_OFFSET(2)], ')')[SAFE_OFFSET(0)]) AS STRING)
  END AS sigla_uf,
SAFE_CAST(domicilios_particulares_permanentes_ocupados_localizados_em_terras_indigenas_domicilios_ AS INT64) domicilios,
SAFE_CAST(moradores_em_domicilios_particulares_permanentes_ocupados_localizados_em_terras_indigenas_pessoas_ AS INT64) moradores,
SAFE_CAST(moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_localizados_em_terras_indigenas_pessoas_ AS INT64) moradores_indigenas,
#SAFE_CAST(REPLACE(media_de_moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_localizados_em_terras_indigenas_pessoas_, ",", ".") AS FLOAT64) media_moradores_indigenas_domicilios_terras_indigenas,
FROM basedosdados-staging.br_ibge_censo_2022_staging.domicilios_moradores_terras_indigenas AS t

