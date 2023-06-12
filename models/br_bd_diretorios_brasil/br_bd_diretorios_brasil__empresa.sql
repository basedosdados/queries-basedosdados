{{ 
  config(
    alias='empresa',    
    schema='br_bd_diretorios_brasil',
    materialized='table',
    cluster_by =    ['id_municipio', 'sigla_uf'],
    labels = {'project_id': 'basedosdados', 'tema': 'economia'})
}}

WITH  max_dia AS (

    SELECT 
      cnpj,
      MAX(data) AS max_data
    FROM `basedosdados.br_me_cnpj.estabelecimentos`
    GROUP BY cnpj
), estabelecimento AS (
SELECT 
    distinct a.cnpj,
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    nome_fantasia,
    cnae_fiscal_principal,
    cnae_fiscal_secundaria,
    CASE 
      WHEN sigla_uf = 'BR' THEN 'RJ'
      ELSE sigla_uf
    END sigla_uf,
    id_pais as id_pais_me,
    CASE
      WHEN a.id_pais = '8' THEN 'Brasil'
      WHEN a.id_pais = '9' THEN 'Brasil'
      WHEN id_pais IS NULL AND sigla_uf IN ('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE',
        'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS','MT','GO','DF','BR') THEN 'Brasil'
      ELSE no_pais 
    END nome_pais_me,
    CASE 
      WHEN a.id_pais = '8' THEN 'BRA'
      WHEN a.id_pais = '9' THEN 'BRA'
      WHEN id_pais IS NULL AND sigla_uf IN ('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE',
        'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS','MT','GO','DF','BR') THEN 'BRA' 
      WHEN a.id_pais IS NULL AND sigla_uf NOT IN ('RO','AC','AM','RR','PA','AP','TO','MA','PI','CE','RN','PB','PE',
        'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS','MT','GO','DF','BR') THEN code_iso3
      ELSE co_pais_isoa3
    END id_code_iso3,
    b.valor AS matriz_filial,
    t.valor AS situacao_cadastral,
    situacao_especial,
    cep,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    a.bairro,
    id_municipio,
    id_municipio_rf,
    CONCAT(ddd_1," ",telefone_1 ) as telefone_1,
    CONCAT(ddd_2," ",telefone_2 ) as telefone_2,
    CONCAT(ddd_fax," ",fax ) as fax, 
    email 

FROM `basedosdados.br_me_cnpj.estabelecimentos` a
INNER JOIN max_dia e
    ON a.cnpj = e.cnpj AND a.data = e.max_data
INNER JOIN `basedosdados.br_me_cnpj.dicionario` b
    ON a.identificador_matriz_filial = b.chave
INNER JOIN `basedosdados.br_me_cnpj.dicionario` t
    ON a.identificador_matriz_filial = t.chave
LEFT JOIN `basedosdados-dev.br_bd_diretorios_brasil_staging.bairro_code_iso3` g
    ON a.bairro = g.bairro
LEFT JOIN `basedosdados-dev.br_bd_diretorios_mundo_staging.pais_code` f
    ON a.id_pais = f.co_pais
WHERE b.nome_coluna ='identificador_matriz_filial' and t.nome_coluna ='situacao_cadastral' )
, empresa AS (
SELECT
  distinct a.cnpj_basico,
  razao_social,
  natureza_juridica,
  ente_federativo,
  capital_social,
  b.valor AS porte,
FROM `basedosdados.br_me_cnpj.empresas` a
INNER JOIN (
  SELECT 
    cnpj_basico,
    MAX(data) as max_data
  FROM `basedosdados.br_me_cnpj.empresas`
  GROUP BY 1
) c
ON a.cnpj_basico = c.cnpj_basico AND a.data = c.max_data
INNER JOIN `basedosdados.br_me_cnpj.dicionario` b
ON a.porte = b.chave
WHERE b.nome_coluna ='porte'
), simples AS (
SELECT 
  distinct cnpj_basico,
  opcao_simples,
  opcao_mei
FROM `basedosdados.br_me_cnpj.simples` 
)
SELECT 
    cnpj,
    a.cnpj_basico,
    a.cnpj_ordem,
    cnpj_dv,
    razao_social,
    nome_fantasia,
    natureza_juridica,
    ente_federativo,
    cnae_fiscal_principal,
    cnae_fiscal_secundaria,
    capital_social,
    porte,
    matriz_filial,
    situacao_cadastral,
    situacao_especial,
    opcao_simples,
    opcao_mei,
    cep,
    tipo_logradouro,
    logradouro,
    numero,
    complemento,
    bairro,
    id_municipio,
    a.sigla_uf,
    id_code_iso3,
    id_pais_me,
    nome_pais_me,
    telefone_1,
    telefone_2,
    fax,
    email
FROM estabelecimento a
LEFT JOIN empresa b
ON a.cnpj_basico = b.cnpj_basico
LEFT JOIN simples c
ON a.cnpj_basico = c.cnpj_basico