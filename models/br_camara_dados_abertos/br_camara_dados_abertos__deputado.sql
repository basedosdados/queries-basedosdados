{{ config(alias='deputado',schema='br_camara_dados_abertos') }}
WITH
  sql AS (
  SELECT
    SAFE_CAST(nome AS STRING) nome,
    SAFE_CAST(nome_civil AS STRING) nome_civil,
    SAFE_CAST(data_nascimento AS DATE) data_nascimento,
    SAFE_CAST(data_falecimento AS DATE) data_falecimento,
    REGEXP_EXTRACT(id_deputado, r'/([^/]+)$') AS id_deputado,
    CASE
    WHEN id_municipio_nascimento = 'SAO PAULO' THEN 'São Paulo'
    WHEN id_municipio_nascimento = 'Moji-Mirim' THEN 'Mogi Mirim'
    WHEN id_municipio_nascimento = "São Lourenço D'Oeste" THEN 'São Lourenço do Oeste'
    WHEN id_municipio_nascimento = "Santa Bárbara D'Oeste" THEN "Santa Bárbara d'Oeste"
    WHEN id_municipio_nascimento = "Araióses" THEN "Araioses"
    WHEN id_municipio_nascimento = "Cacador" THEN "Caçador"
    WHEN id_municipio_nascimento = "Pindaré Mirim" THEN "Pindaré-Mirim"
    WHEN id_municipio_nascimento = "Belém de São Francisco" THEN "Belém do São Francisco"
    WHEN id_municipio_nascimento = "Sud Menucci" THEN "Sud Mennucci"
    WHEN id_municipio_nascimento = 'Duerê' THEN "Dueré"
    WHEN id_municipio_nascimento = 'Santana do Livramento' THEN "Sant'Ana do Livramento"
    WHEN id_municipio_nascimento = "Herval D'Oeste" THEN "Herval d'Oeste"
    WHEN id_municipio_nascimento = "Guaçui" THEN "Guaçuí"
    WHEN id_municipio_nascimento = "Lençois Paulista" THEN "Lençóis Paulista"
    WHEN id_municipio_nascimento = "Amambaí" THEN "Amambai"
    WHEN id_municipio_nascimento = "Santo Estevão" THEN "Santo Estêvão"
    WHEN id_municipio_nascimento = "Poxoréu" THEN "Poxoréo"
    WHEN id_municipio_nascimento = "Trajano de Morais" THEN "Trajano de Moraes"
    ELSE id_municipio_nascimento
    END
    AS id_municipio_nascimento,
    SAFE_CAST(sigla_uf_nascimento AS STRING) sigla_uf_nascimento,
    CASE  
      WHEN sexo = 'M' THEN 'Masculino'  
      WHEN sexo = 'F' THEN 'Feminino'  
      ELSE sexo  
    END AS sexo,
    SAFE_CAST(id_inicial_legislatura AS STRING) id_inicial_legislatura,
    SAFE_CAST(id_final_legislatura AS STRING) id_final_legislatura,
    SAFE_CAST(url_site AS STRING) url_site,
    SAFE_CAST(url_rede_social AS STRING) url_rede_social,
  FROM
    basedosdados-staging.br_camara_dados_abertos_staging.deputado),
  uniao_valores AS (
  SELECT
    a.*,
    b.nome AS name_id_municipio,
    b.id_municipio,
    b.sigla_uf
  FROM sql as a
    LEFT JOIN `basedosdados.br_bd_diretorios_brasil.municipio` AS b
    on a.id_municipio_nascimento = b.nome and a.sigla_uf_nascimento = b.sigla_uf)
    select 
    nome,
    nome_civil,
    data_nascimento,
    data_falecimento,
    id_municipio as id_municipio_nascimento,
    sigla_uf_nascimento,
    id_deputado,
    sexo,
    id_inicial_legislatura,
    id_final_legislatura,
    url_site,
    url_rede_social,
    from uniao_valores


