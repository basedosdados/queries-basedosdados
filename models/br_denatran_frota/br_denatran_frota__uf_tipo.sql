{{
config(
    alias='uf_tipo',
    schema='br_denatran_frota',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2003,
        "end": 2024,
        "interval": 1}},
    cluster_by = ["mes", "sigla_uf"]
)
}}


with tipo_mun as (
SELECT
    ano,
    mes,
    sigla_uf,
    CASE
    WHEN tipo_veiculo = 'AUTOMÓVEL' THEN 'AUTOMOVEL'
    WHEN tipo_veiculo = 'CAMINHÃO' THEN 'CAMINHAO'
    WHEN tipo_veiculo = 'CAMINHÃO TRATOR' THEN 'CAMINHAO TRATOR'
    WHEN tipo_veiculo = 'CHASSI PLATAFAFORMA' THEN 'CHASSI PLATAFORMA'
    WHEN tipo_veiculo = 'CHASSI PLATAF' THEN 'CHASSI PLATAFORMA'
    WHEN tipo_veiculo = 'MICRO-ÔNIBUS' THEN 'MICRO-ONIBUS'
    WHEN tipo_veiculo = 'MICROÔNIBUS' THEN 'MICRO-ONIBUS'
    WHEN tipo_veiculo = 'ÔNIBUS' THEN 'ONIBUS'
    WHEN tipo_veiculo = 'UTILITÁRIO' THEN 'UTILITARIO'
    WHEN tipo_veiculo = 'nan' THEN ''
    WHEN tipo_veiculo = 'TRATOR ESTEI' THEN 'TRATOR ESTEIRA'
    ELSE tipo_veiculo
    END as tipo_veiculo2,
    CAST(REPLACE(quantidade, '.0', '') as INT64) quantidade
FROM basedosdados-staging.br_denatran_frota_staging.uf_tipo
), 
nova_agregacao as (
SELECT 
ano,
mes,
sigla_uf,
tipo_veiculo2 as tipo_veiculo,
SUM(quantidade) as quantidade
FROM tipo_mun
GROUP BY 1,2,3,4
)
SELECT
    SAFE_CAST(ano as INT64) ano,
    SAFE_CAST(mes as INT64) mes,
    SAFE_CAST(sigla_uf as STRING) sigla_uf,
    SAFE_CAST(tipo_veiculo as STRING) tipo_veiculo,
    SAFE_CAST(quantidade as INT64) quantidade
FROM nova_agregacao