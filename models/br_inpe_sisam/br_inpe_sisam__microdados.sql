
{{ 
    config(
        alias='microdados', 
        schema='br_inpe_sisam',
    partition_by = {
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2000,
        "end": 2025,
        "interval": 1}
     },
    cluster_by = ["ano", "sigla_uf"],
    labels = {'tema': 'meio-ambiente'}
    )
 }}
SELECT
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(data_hora AS DATETIME) data_hora,
SAFE_CAST(co_ppb AS FLOAT64) co_ppb,
SAFE_CAST(no2_ppb AS FLOAT64) no2_ppb,
SAFE_CAST(o3_ppb AS FLOAT64) o3_ppb,
SAFE_CAST(pm25_ugm3 AS FLOAT64) pm25_ugm3,
SAFE_CAST(so2_ugm3 AS FLOAT64) so2_ugm3,
SAFE_CAST(precipitacao_dia AS FLOAT64) precipitacao_dia,
SAFE_CAST(temperatura AS FLOAT64) temperatura,
SAFE_CAST(umidade_relativa AS FLOAT64) umidade_relativa,
SAFE_CAST(vento_direcao AS INT64) vento_direcao,
SAFE_CAST(vento_velocidade AS FLOAT64) vento_velocidade,
FROM basedosdados-staging.br_inpe_sisam_staging.microdados AS t