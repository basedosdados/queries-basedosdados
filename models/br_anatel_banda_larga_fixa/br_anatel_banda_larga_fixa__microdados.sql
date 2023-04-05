{{ config(alias='microdados', schema='br_anatel_banda_larga_fixa') }}

SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(id_municipio AS STRING) id_municipio,
SAFE_CAST(cnpj AS STRING) cnpj,
SAFE_CAST(empresa AS STRING) empresa,
SAFE_CAST(porte_empresa AS STRING) porte_empresa,
SAFE_CAST(tecnologia AS STRING) tecnologia,
SAFE_CAST(transmissao AS STRING) transmissao,
SAFE_CAST(velocidade AS STRING) velocidade,
SAFE_CAST(produto AS STRING) produto,
SAFE_CAST(acessos AS INT64) acessos
FROM basedosdados-dev.br_anatel_banda_larga_fixa_staging.microdados AS t