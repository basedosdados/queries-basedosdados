{{ config(alias='indigenas_domicilio_pelo_menos_um_morador_indigena_terra_indigena',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(cod_ AS STRING) id_terra_indigena,
SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')) AS STRING) terra_indigena,
SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_domicilios_ AS INT64) domicilios,
SAFE_CAST(moradores_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_pessoas_ AS INT64) moradores,
SAFE_CAST(moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_pessoas_ AS INT64) moradores_indigenas,
#SAFE_CAST(REPLACE(media_de_moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_pessoas_, ",", ".") AS FLOAT64) media_moradores_indigenas_domicilios_terras_indigenas_pelo_menos_um,
FROM basedosdados-staging.br_ibge_censo_2022_staging.indigenas_domicilio_pelo_menos_um_morador_indigena_terra_indigena AS t
