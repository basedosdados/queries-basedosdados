{{ config(
    materialized='table',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 1995,
        "end": 2023,
        "interval": 1
      }
    }
)}}


SELECT 

SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(data_cadastramento AS DATE) data_cadastramento,
SAFE_CAST(numero_contribuinte AS STRING) numero_contribuinte,
SAFE_CAST(numero_notificacao AS STRING) numero_notificacao,
SAFE_CAST(numero_condominio AS STRING) numero_condominio,
SAFE_CAST(codigo_logradouro AS STRING) codigo_logradouro,
SAFE_CAST(logradouro AS STRING) logradouro,
SAFE_CAST(numero_imovel AS INT64) numero_imovel,
SAFE_CAST(cep AS STRING) cep,
SAFE_CAST(complemento AS STRING) complemento,
SAFE_CAST(bairro AS STRING) bairro,
SAFE_CAST(referencia_imovel AS STRING) referencia_imovel,
SAFE_CAST(finalidade_imovel AS STRING) finalidade_imovel,
SAFE_CAST(tipo_construcao AS STRING) tipo_construcao,
SAFE_CAST(tipo_terreno AS STRING) tipo_terreno,
SAFE_CAST(fracao_ideal AS FLOAT64) fracao_ideal,
SAFE_CAST(area_terreno AS INT64) area_terreno,
SAFE_CAST(area_construida AS INT64) area_construida,
SAFE_CAST(area_ocupada AS INT64) area_ocupada,
SAFE_CAST(testada_imovel AS FLOAT64) testada_imovel,
SAFE_CAST(quantidade_esquina_imovel AS STRING) quantidade_esquina_imovel,
SAFE_CAST(valor_terreno AS INT64) valor_terreno,
SAFE_CAST(valor_construcao AS INT64) valor_construcao,
SAFE_CAST(ano_construcao_corrigida AS INT64) ano_construcao_corrigida,
SAFE_CAST(quantidade_pavimento AS INT64) quantidade_pavimento,
SAFE_CAST(ano_inicio_vida_contribuinte AS INT64) ano_inicio_vida_contribuinte,
SAFE_CAST(mes_inicio_vida_contribuinte AS INT64) mes_inicio_vida_contribuinte,
SAFE_CAST(fator_obsolescencia AS FLOAT64) fator_obsolescencia

FROM basedosdados-staging.br_sp_saopaulo_geosampa_iptu_staging.iptu AS t
