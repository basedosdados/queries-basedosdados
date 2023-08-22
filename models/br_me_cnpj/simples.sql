{{
  config(
    schema='br_me_cnpj',
    materialized='table',
  )
}}

SELECT 
  SAFE_CAST(lpad(cnpj_basico, 8, '0') AS STRING) cnpj_basico,
  SAFE_CAST(opcao_simples AS INT64) opcao_simples,
  SAFE_CAST(data_opcao_simples AS DATE) data_opcao_simples,
  SAFE_CAST(data_exclusao_simples AS DATE) data_exclusao_simples,
  SAFE_CAST(opcao_mei AS INT64) opcao_mei,
  SAFE_CAST(data_opcao_mei AS DATE) data_opcao_mei,
  SAFE_CAST(data_exclusao_mei AS DATE) data_exclusao_mei
FROM basedosdados-staging.br_me_cnpj_staging.simples AS t
WHERE opcao_mei != "opcao_mei"