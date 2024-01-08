{{ config(alias='populacao_residente_cor_raca_municipio',schema='br_ibge_censo_2022') }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(cod_ AS STRING) id_municipio,
SAFE_CAST(idade AS STRING) grupo_idade,
SAFE_CAST(sexo AS STRING) sexo,
SAFE_CAST(cor_ou_raca AS STRING) cor_raca,
SAFE_CAST(populacao_residente_pessoas_ AS INT64) populacao_residente,
FROM basedosdados-dev.br_ibge_censo_2022_staging.populacao_residente_cor_raca_municipio AS t

