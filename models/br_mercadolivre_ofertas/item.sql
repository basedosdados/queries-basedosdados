select 
parse_datetime('%Y-%m-%d %H:%M:%S', data_hora) as data_hora,
titulo,
lpad(item_id, 12, '0') as item_id,
if(
    trim(regexp_replace(categorias, r'\[|\]|\'', '')) = '', 
    null, 
    array(
      select trim(value)
      from unnest(split(regexp_replace(categorias, r'\[|\]|\'', ''))) as value
    )
  ) as categorias,
SAFE_CAST(quantidade_avaliacoes AS INT64) quantidade_avaliacoes,
SAFE_CAST(desconto AS INT64) desconto,
SAFE_CAST(envio_pais AS BOOL) envio_pais,
SAFE_CAST(estrelas AS FLOAT64) estrelas,
SAFE_CAST(preco AS FLOAT64) preco,
SAFE_CAST(preco_original AS FLOAT64) preco_original,
vendedor,
secao_site,
caracteristicas,
from `basedosdados-staging.br_mercadolivre_ofertas_staging.item`