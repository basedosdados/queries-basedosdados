{{ config(alias='quilombolas_domicilio_pelo_menos_um_morador_quilombola_territorio_quilombola',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(TRIM(REGEXP_EXTRACT(territorio_quilombola_por_unidade_da_federacao, r'([^\(]+)')) AS STRING) territorio_quilombola,
SAFE_CAST(TRIM(REGEXP_EXTRACT(territorio_quilombola_por_unidade_da_federacao, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_domicilios_ AS STRING) domicilios,
SAFE_CAST(moradores_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_ AS INT64) moradores,
SAFE_CAST(moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_quilombola_pessoas_ AS INT64) moradores_quilombolas,
FROM basedosdados-staging.br_ibge_censo_2022_staging.domicilios_pelo_menos_um_territorios_quilombolas AS t
