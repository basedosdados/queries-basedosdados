{{ config(alias='indigenas_populacao_residente_terras_indigenas',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')) AS STRING) terra_indigena,
SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'\(([^)]+)\)')) AS STRING) sigla_uf,
SAFE_CAST(pessoas_residentes_em_terras_indigenas_pessoas_ AS INT64) populacao_residente_residentes,
SAFE_CAST(pessoas_indigenas_residentes_em_terras_indigenas_pessoas_ AS INT64) pessoas_indigenas,
SAFE_CAST(quesito_de_declaracao_indigena AS STRING) quesito_declaracao_indigena,
FROM basedosdados-dev.br_ibge_censo_2022_staging.populacao_residente_terras_indigenas AS t
