{{
  config(
    alias = 'recurso_publico_propriedade',
    schema='br_bcb_sicor',
    materialized='table',
  )
}}

SELECT
SAFE_CAST(id_referencia_bacen AS STRING) id_referencia_bacen,
SAFE_CAST(numero_ordem AS STRING) numero_ordem,
SAFE_CAST(tipo_cpf_cnpj AS STRING) tipo_cpf_cnpj,
SAFE_CAST(id_sncr AS STRING) id_sncr,
SAFE_CAST(id_nirf AS STRING) id_nirf,
SAFE_CAST(id_car AS STRING) id_car
FROM basedosdados-staging.br_bcb_sicor_staging.recurso_publico_propriedade AS t