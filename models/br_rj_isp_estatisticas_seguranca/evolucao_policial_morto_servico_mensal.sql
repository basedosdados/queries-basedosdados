SELECT 

SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(id_cisp AS STRING) id_cisp,
SAFE_CAST(quantidade_policial_militar_morto_servico AS INT64) quantidade_policial_militar_morto_servico,
SAFE_CAST(quantidade_policial_civil_morto_servico AS INT64) quantidade_policial_civil_morto_servico

FROM basedosdados-staging.br_rj_isp_estatisticas_seguranca_staging.evolucao_policial_morto_servico_mensal AS t
