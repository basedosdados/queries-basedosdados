{{
    config(
        materialized='incremental',
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        }    
    )
}}

SELECT 
  *
FROM basedosdados-dev.br_tse_eleicoes_2022_staging.resultado_secao AS t
