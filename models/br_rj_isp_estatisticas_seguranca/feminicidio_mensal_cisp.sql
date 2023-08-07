SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_cisp AS STRING) id_cisp,
SAFE_CAST(quantidade_morte_feminicidio AS INT64) quantidade_morte_feminicidio,
SAFE_CAST(quantidade_tentativa_feminicidio AS INT64) quantidade_tentativa_feminicidio,
SAFE_CAST(tipo_fase AS STRING) tipo_fase
FROM basedosdados-staging.br_rj_isp_estatisticas_seguranca_staging.feminicidio_mensal_cisp AS t