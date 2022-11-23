SELECT
SAFE_CAST(id AS STRING) id,
SAFE_CAST(nome AS STRING) nome,
SAFE_CAST(descricao AS STRING) descricao,
SAFE_CAST(email AS STRING) email,
SAFE_CAST(twitter AS STRING) twitter,
SAFE_CAST(github AS STRING) github,
SAFE_CAST(website AS STRING) website,
SAFE_CAST(linkedin AS STRING) linkedin,
SAFE_CAST(url_foto AS STRING) url_foto
FROM basedosdados-staging.br_bd_indicadores_staging.pessoas AS t