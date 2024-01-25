{{
config(
    alias='municipio_tipo',
    schema='br_denatran_frota',
    materialization='incremental',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2003,
        "end": 2024,
        "interval": 1}},
    cluster_by = ["mes"]
)

}}

with tipo_municipio as (
SELECT
    ano,
    mes,
    sigla_uf,
    id_municipio,
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
    quantidade
    FROM basedosdados-dev.br_denatran_frota_staging.municipio_tipo
)

SELECT
    SAFE_CAST(ano as INT64) ano,
    SAFE_CAST(mes as INT64) mes,
    SAFE_CAST(sigla_uf as STRING) sigla_uf,
    SAFE_CAST(id_municipio as STRING) id_municipio,
    SAFE_CAST(LOWER(tipo_veiculo2) as STRING) tipo_veiculo,
    SAFE_CAST(quantidade as INT64) quantidade
FROM tipo_municipio
{% if is_incremental() %} 
WHERE DATE(CAST(ano AS INT64),CAST(mes AS INT64),1) > (SELECT MAX(DATE(CAST(ano AS INT64),CAST(mes AS INT64),1)) FROM {{ this }} )
{% endif %}