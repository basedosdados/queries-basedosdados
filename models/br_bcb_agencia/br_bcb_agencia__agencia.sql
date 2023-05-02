{{ config(alias = 'agencia', schema = 'br_bcb_agencia')}}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(data_inicio AS DATE) data_inicio,
SAFE_CAST(cnpj AS STRING) cnpj,
SAFE_CAST(nome_agencia AS STRING) nome_agencia,
SAFE_CAST(instituicao AS STRING) instituicao,
SAFE_CAST(segmento AS STRING) segmento,
SAFE_CAST(id_compe_bcb_agencia AS STRING) id_compe_bcb_agencia,
SAFE_CAST(id_compe_bcb_instituicao AS STRING) id_compe_bcb_instituicao,
SAFE_CAST(cep AS STRING) cep,
SAFE_CAST(endereco AS STRING) endereco,
SAFE_CAST(complemento AS STRING) complemento,
SAFE_CAST(bairro AS STRING) bairro,
SAFE_CAST(ddd AS STRING) ddd,
SAFE_CAST(fone AS STRING) fone,
SAFE_CAST(id_instalacao AS STRING) id_instalacao
FROM basedosdados-dev.br_bcb_agencia_staging.agencia AS t
