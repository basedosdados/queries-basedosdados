{{ config(alias='quilombolas_populacao_residente_territorios_quilombolas',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(TRIM(REGEXP_EXTRACT(territorio_quilombola_por_unidade_da_federacao, r'([^\(]+)')) AS STRING) territorio_quilombola,
SAFE_CAST(TRIM(REGEXP_EXTRACT(territorio_quilombola_por_unidade_da_federacao, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(pessoas_quilombolas_residentes_em_territorios_quilombolas_pessoas_ AS INT64) pessoas_quilombolas,
SAFE_CAST(pessoas_residentes_em_territorios_quilombolas_pessoas_ AS INT64) populacao_residente,
FROM basedosdados-dev.br_ibge_censo_2022_staging.populacao_residente_territorios_quilombolas AS t