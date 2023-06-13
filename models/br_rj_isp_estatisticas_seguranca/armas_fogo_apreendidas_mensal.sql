SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(id_cisp AS STRING) id_cisp,
SAFE_CAST(id_aisp AS STRING) id_aisp,
SAFE_CAST(id_risp AS STRING) id_risp,
SAFE_CAST(quantidade_arma_fogo_apreendida AS INT64) quantidade_arma_fogo_apreendida

FROM basedosdados-dev.br_rj_isp_estatisticas_seguranca_staging.armas_fogo_apreendidas_mensal AS t