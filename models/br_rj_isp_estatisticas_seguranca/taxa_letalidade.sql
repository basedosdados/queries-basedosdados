SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(regiao AS INT64) regiao,
SAFE_CAST(delito AS STRING) delito,
SAFE_CAST(contagem_delito AS INT64) contagem_delito,
SAFE_CAST(populacao AS INT64) populacao,
SAFE_CAST(taxa_cem_mil_habitantes AS INT64) taxa_cem_mil_habitantes
FROM basedosdados-staging.br_rj_isp_estatisticas_seguranca_staging.taxa_letalidade AS t
