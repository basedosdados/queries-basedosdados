SELECT 
SAFE_CAST(id_empreendimento AS STRING) id,
SAFE_CAST(data_inicio_empreendimento AS DATE) data_inicio,
SAFE_CAST(data_fim_empreendimento AS DATE) data_fim,
SAFE_CAST(finalidade AS STRING) finalidade,
SAFE_CAST(atividade AS STRING) atividade,
SAFE_CAST(modalidade AS STRING) modalidade,
SAFE_CAST(produto AS STRING) produto,
SAFE_CAST(variedade AS STRING) variedade,
SAFE_CAST(cesta_safra AS STRING) cesta_safra,
SAFE_CAST(zoneamento AS STRING) zoneamento,
SAFE_CAST(unidade_medida AS STRING) unidade_medida,
SAFE_CAST(unidade_medida_previsao_producao AS STRING) unidade_medida_previsao_producao,
SAFE_CAST(consorcio AS STRING) consorcio,
SAFE_CAST(cedula_mae AS STRING) cedula_mae,
SAFE_CAST(id_tipo_cultura AS STRING) id_tipo_cultura
FROM basedosdados.test_dataset_staging.test_table_laura AS t