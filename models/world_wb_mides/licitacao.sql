{{
  config(
    schema = "world_wb_mides",
    materialized = "table",
    partition_by = {
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2009,
        "end": 2021,
        "interval": 1}
    },
    cluster_by = ["mes", "sigla_uf"],
    labels = {"project_id": "basedosdados", "tema": "economia"}
  )
 }}
SELECT
    SAFE_CAST(ano AS INT64) ano,
    SAFE_CAST(mes AS INT64) mes,
    SAFE_CAST(sigla_uf AS STRING) sigla_uf,
    SAFE_CAST(id_municipio AS STRING) id_municipio,
    SAFE_CAST(orgao AS STRING) orgao,
    SAFE_CAST(id_unidade_gestora AS STRING) id_unidade_gestora,
    SAFE_CAST(id_licitacao_bd AS STRING) id_licitacao_bd,
    SAFE_CAST(id_licitacao AS STRING) id_licitacao,
    SAFE_CAST(id_dispensa AS STRING) id_dispensa,
    SAFE_CAST(ano_processo AS INT64) ano_processo,
    SAFE_CAST(data_abertura AS DATE) data_abertura,
    SAFE_CAST(data_edital AS DATE) data_edital,
    SAFE_CAST(data_homologacao AS DATE) data_homologacao,
    SAFE_CAST(data_publicacao_dispensa AS DATE) data_publicacao_dispensa,
    SAFE_CAST(descricao_objeto AS STRING) descricao_objeto,
    SAFE_CAST(natureza_objeto AS STRING) natureza_objeto,
    SAFE_CAST(modalidade AS STRING) modalidade,
    SAFE_CAST(natureza_processo AS STRING) natureza_processo,
    SAFE_CAST(tipo AS STRING) tipo,
    SAFE_CAST(forma_pagamento AS STRING) forma_pagamento,
    SAFE_CAST(valor_orcamento AS FLOAT64) valor_orcamento,
    SAFE_CAST(valor AS FLOAT64) valor,
    SAFE_CAST(valor_corrigido AS FLOAT64) valor_corrigido,
    SAFE_CAST(situacao AS STRING) situacao,
    SAFE_CAST(estagio AS STRING) estagio,
    SAFE_CAST(preferencia_micro_pequena AS STRING) preferencia_micro_pequena,
    SAFE_CAST(exclusiva_micro_pequena AS STRING) exclusiva_micro_pequena,
    SAFE_CAST(contratacao AS STRING) contratacao,
    SAFE_CAST(quantidade_convidados AS INT64) quantidade_convidados,
    SAFE_CAST(tipo_cadastro AS STRING) tipo_cadastro,
    SAFE_CAST(carona AS STRING) carona,
    SAFE_CAST(covid_19 AS STRING) covid_19
FROM `basedosdados-staging.world_wb_mides_staging.licitacao` AS t