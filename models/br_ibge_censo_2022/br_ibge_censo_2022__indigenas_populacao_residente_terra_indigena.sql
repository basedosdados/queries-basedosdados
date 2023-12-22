{{ config(alias='indigenas_populacao_residente_terra_indigena',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(cod_ AS STRING) id_terra_indigena,
SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')) AS STRING) terra_indigena,
  CASE
    WHEN REGEXP_CONTAINS(terra_indigena_por_unidade_da_federacao, r'\(\w{2}\)') THEN
      SAFE_CAST(TRIM(REGEXP_EXTRACT(terra_indigena_por_unidade_da_federacao, r'\((\w{2})\)')) AS STRING)
    ELSE
      SAFE_CAST(TRIM(SPLIT(SPLIT(terra_indigena_por_unidade_da_federacao, '(')[SAFE_OFFSET(2)], ')')[SAFE_OFFSET(0)]) AS STRING)
  END AS sigla_uf,
SAFE_CAST(pessoas_residentes_em_terras_indigenas_pessoas_ AS INT64) populacao_residente,
SAFE_CAST(pessoas_indigenas_residentes_em_terras_indigenas_pessoas_ AS INT64) pessoas_indigenas,
SAFE_CAST(quesito_de_declaracao_indigena AS STRING) quesito_declaracao_indigena,
FROM basedosdados-staging.br_ibge_censo_2022_staging.indigenas_populacao_residente_terra_indigena AS t



