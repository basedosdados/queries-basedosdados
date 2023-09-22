{{
  config(
    alias = 'recurso_publico_cooperado',
    schema='br_bcb_sicor',
    materialized='table',
  )
}}

SELECT
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(numero_ordem AS STRING) numero_ordem,
SAFE_CAST(tipo_cpf_cnpj AS STRING) tipo_cpf_cnpj,
SAFE_CAST(tipo_pessoa AS STRING) tipo_pessoa,
SAFE_CAST(valor_parcela AS FLOAT64) valor_parcela
FROM basedosdados-staging.br_bcb_sicor_staging.recurso_publico_cooperado AS t