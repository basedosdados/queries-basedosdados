{{
  config(
    alias = 'recurso_publico_mutuario',
    schema='br_bcb_sicor',
    materialized='table',
  )
}}


SELECT
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(indicador_sexo AS INT64) indicador_sexo,
SAFE_CAST(tipo_cpf_cnpj AS STRING) tipo_cpf_cnpj,
SAFE_CAST(tipo_beneficiario AS STRING) tipo_beneficiario,
SAFE_CAST(id_dap AS STRING) id_dap
FROM basedosdados-staging.br_bcb_sicor_staging.recurso_publico_mutuario AS t