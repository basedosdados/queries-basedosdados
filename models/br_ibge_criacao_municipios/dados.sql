SELECT 
  t1.id_municipio_criado,
  t1.nome_municipio_criado,
  t1.id_municipio_origem,
  t2.nome AS nome_municipio_origem,
  t1.lei,
  safe_cast(t1.data_criacao AS date) AS data_criacao,
  safe_cast(t1.data_instalacao AS date) AS data_instalacao, 
FROM `basedosdados-staging.br_ibge_criacao_municipios_staging.dados` AS t1
JOIN `basedosdados.br_bd_diretorios_brasil.municipio` AS t2
ON t1.id_municipio_origem = t2.id_municipio