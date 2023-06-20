SELECT 

SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_cisp AS STRING) id_cisp,
SAFE_CAST(id_aisp AS STRING) id_aisp,
SAFE_CAST(id_risp AS STRING) id_risp,
SAFE_CAST(quantidade_arma_fabricacao_caseira AS INT64) quantidade_arma_fabricacao_caseira,
SAFE_CAST(quantidade_carabina AS INT64) quantidade_carabina,
SAFE_CAST(quantidade_espingarda AS INT64) quantidade_espingarda,
SAFE_CAST(quantidade_fuzil AS INT64) quantidade_fuzil,
SAFE_CAST(quantidade_garrucha AS INT64) quantidade_garrucha,
SAFE_CAST(quantidade_garruchao AS INT64) quantidade_garruchao,
SAFE_CAST(quantidade_metralhadora AS INT64) quantidade_metralhadora,
SAFE_CAST(quantidade_outros AS INT64) quantidade_outros,
SAFE_CAST(quantidade_pistola AS INT64) quantidade_pistola,
SAFE_CAST(quantidade_revolver AS INT64) quantidade_revolver,
SAFE_CAST(quantidade_submetralhadora AS INT64) quantidade_submetralhadora,
SAFE_CAST(total AS INT64) total
FROM basedosdados-staging.br_rj_isp_estatisticas_seguranca_staging.armas_apreendidas_mensal AS t