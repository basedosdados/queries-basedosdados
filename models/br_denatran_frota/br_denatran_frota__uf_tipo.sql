{{
config(
    alias='uf_tipo',
    schema='br_denatran_frota',
    materialization='table'
)
}}


with uf_tipo as (
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
    WHEN tipo_veiculo = 'caminhaotrator' THEN 'caminhao trator'
    WHEN tipo_veiculo = 'chassiplataforma' THEN 'chassi plataforma'
    WHEN tipo_veiculo = 'moto cicleta' THEN 'motocicleta'
    WHEN tipo_veiculo = 'moto  cicleta' THEN 'motocicleta'
    WHEN tipo_veiculo = 'MICRO-ÔNIBUS' THEN 'MICRO-ONIBUS'
    WHEN tipo_veiculo = 'microonibus' THEN 'micro-onibus'
    WHEN tipo_veiculo = 'sidecar' THEN 'side-car'
    WHEN tipo_veiculo = 'semireboque' THEN 'semi-reboque'
    WHEN tipo_veiculo = 'tratoresteira' THEN 'trator esteira'
    WHEN tipo_veiculo = 'tratorrodas' THEN 'trator rodas'
    WHEN tipo_veiculo = 'MICROÔNIBUS' THEN 'MICRO-ONIBUS'
    WHEN tipo_veiculo = 'ÔNIBUS' THEN 'ONIBUS'
    WHEN tipo_veiculo = 'UTILITÁRIO' THEN 'UTILITARIO'
    WHEN tipo_veiculo = 'nan' THEN ''
    WHEN tipo_veiculo = 'TRATOR ESTEI' THEN 'TRATOR ESTEIRA'
    ELSE tipo_veiculo
    END as tipo_veiculo2,
    quantidade
FROM basedosdados-dev.br_denatran_frota_staging.uf_tipo
)

SELECT
    SAFE_CAST(ano as INT64) ano,
    SAFE_CAST(mes as INT64) mes,
    SAFE_CAST(sigla_uf as STRING) sigla_uf,
    SAFE_CAST(LOWER(tipo_veiculo2) as STRING) tipo_veiculo,
    SAFE_CAST(quantidade as INT64) quantidade
FROM uf_tipo