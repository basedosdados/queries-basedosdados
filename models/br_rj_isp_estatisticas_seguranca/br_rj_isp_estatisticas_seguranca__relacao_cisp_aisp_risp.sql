{{ config(alias='relacao_cisp_aisp_risp',schema='br_rj_isp_estatisticas_seguranca') }}

SELECT 
SAFE_CAST(id_risp AS STRING) id_risp,
SAFE_CAST(id_aisp AS STRING) id_aisp,
SAFE_CAST(id_cisp AS STRING) id_cisp,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(unidade_territorial AS STRING) unidade_territorial,
SAFE_CAST(regiao AS STRING) regiao
FROM basedosdados-staging.br_rj_isp_estatisticas_seguranca_staging.relacao_cisp_aisp_risp AS t