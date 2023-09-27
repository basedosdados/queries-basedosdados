{{ 
config(
    schema='br_stf_corte_aberta',
    alias='decisoes',
    materialized='table',
    partition_by={
    "field": "ano",
    "data_type": "int64",
    "range": {
        "start": 2000,
        "end": 2023,
        "interval": 1}
    },
    labels =  {'tema': 'direito'},
    )
}}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(classe AS STRING) classe,
SAFE_CAST(numero AS STRING) numero,
INITCAP(relator) relator,
SAFE_CAST(link AS STRING) link,
INITCAP(subgrupo_andamento) subgrupo_andamento,
INITCAP(andamento) andamento,
INITCAP(observacao_andamento_decisao) observacao_andamento_decisao,
INITCAP(modalidade_julgamento) modalidade_julgamento,
INITCAP(tipo_julgamento) tipo_julgamento,
INITCAP(meio_tramitacao) meio_tramitacao,
SAFE_CAST(indicador_tramitacao AS BOOL) indicador_tramitacao,
INITCAP(assunto_processo) assunto_processo,
INITCAP(ramo_direito) ramo_direito,
SAFE_CAST(data_autuacao AS DATE) data_autuacao,
SAFE_CAST(data_decisao AS DATE) data_decisao,
SAFE_CAST(data_baixa_processo AS DATE) data_baixa_processo
FROM basedosdados-staging.br_stf_corte_aberta_staging.decisoes AS t