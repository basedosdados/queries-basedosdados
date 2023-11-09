{{
  config(
    alias = 'recurso_publico_complemento_operacao',
    schema='br_bcb_sicor',
    materialized='table',
    partition_by = {
      "field": "id_municipio",
      "data_type": "string"
    }
  )
}}

SELECT
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(numero_ordem AS STRING) numero_ordem,
SAFE_CAST(id_referencia_bacen_efetivo AS STRING) id_referencia_bacen_efetivo,
SAFE_CAST(id_agencia AS STRING) id_agencia
FROM basedosdados-staging.br_bcb_sicor_staging.recurso_publico_complemento_operacao AS t