{{ 
  config(
    schema='br_rf_cafir',
    alias='imoveis_rurais',
    materialized='incremental',
    partition_by={
      "field": "data_referencia",
      "data_type": "date",
      "granularity": "day"
     },
    cluster_by=['sigla_uf'],
    post_hook=['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(data_referencia), MONTH) > 6)',
              'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (EXTRACT(YEAR from data_referencia) = EXTRACT(YEAR from  CURRENT_DATE()))' ]
  )   
 }}

with lower_munis as (
  SELECT 
  *,
  LOWER(municipio) as nome_mun, 
  FROM basedosdados-staging.br_rf_cafir_staging.imoveis_rurais 
),
fixed_names as (
  SELECT 
  CASE
    WHEN nome_mun = 'lagoa do itaenga' THEN 'lagoa de itaenga'
    WHEN nome_mun = 'itapaje' THEN 'itapage'
    WHEN nome_mun = "olho d'agua do borges" THEN 	"olho-d'agua do borges"
    WHEN nome_mun = 'graccho cardoso' THEN 'gracho cardoso'
    WHEN nome_mun = 'passa vinte' THEN 'passa-vinte'
    WHEN nome_mun = 'parati' THEN 'paraty'
    WHEN nome_mun =  'balneario de picarras' THEN 'balneario picarras'
    WHEN nome_mun =  'mogi-guacu' THEN 'mogi guacu' 
    WHEN nome_mun = 'sao luiz do paraitinga' THEN 'sao luis do paraitinga'
    WHEN nome_mun = 'santana do livramento' THEN "sant'ana do livramento"
    WHEN nome_mun = 'belem de sao francisco' THEN 'belem do sao francisco'
    WHEN nome_mun = 'barao do monte alto' THEN 'barao de monte alto'
    WHEN nome_mun = 'sao tome das letras' THEN 'sao thome das letras'
    WHEN nome_mun = 'brasopolis' THEN 'brazopolis'
    WHEN nome_mun = 'florinea' THEN 'florinia'
    WHEN nome_mun = 'sao valerio da natividade' THEN 'sao valerio'
    WHEN nome_mun = 'santa cruz do monte castelo' THEN 'santa cruz de monte castelo'
    WHEN nome_mun = 'poxoreu' THEN 'poxoreo'
    WHEN nome_mun = 'pindare mirim' THEN 'pindare-mirim'
    WHEN nome_mun = 'entre ijuis' THEN 'entre-ijuis'
    WHEN nome_mun = 'assu' THEN 'acu'
    WHEN nome_mun = 'amparo da serra' THEN 'amparo do serra'
    WHEN nome_mun = 'dona euzebia' THEN 'dona eusebia'
    WHEN nome_mun = 'eldorado dos carajas' THEN 'eldorado do carajas'
    WHEN nome_mun = 'couto de magalhaes' THEN 'couto magalhaes'
    WHEN nome_mun = 'sao domingos de pombal' THEN 'sao domingos'
    WHEN nome_mun = 'picarras' THEN 'balneario picarras'
    WHEN nome_mun = "pingo d'agua" THEN "pingo-d'agua"
    WHEN nome_mun = 'suzanopolis' THEN 'suzanapolis'
    WHEN nome_mun = 'suzanopolis' THEN 'suzanapolis'
    WHEN nome_mun = 'povoado pouso alegre' THEN  'pouso alegre'
    WHEN nome_mun = 'alta floresta d oeste' THEN "alta floresta d'oeste"
    WHEN nome_mun = 'santa luzia d oeste' THEN "santa luzia d'oeste"
    WHEN nome_mun = "machadinho d oeste" THEN "machadinho d'oeste"
    WHEN nome_mun = "gloria d oeste" THEN "gloria d'oeste"
    WHEN nome_mun = "alvorada d oeste" THEN "alvorada d'oeste"
    WHEN nome_mun = "bom jesus" AND sigla_uf = 'GO' THEN "bom jesus de goias"
    WHEN nome_mun = "presidente castelo branco" AND sigla_uf = 'SC' THEN 'presidente castello branco'
    WHEN nome_mun = "santarem" AND sigla_uf = 'PB' THEN 'joca claudino'
    ELSE nome_mun
    END as nome_mun,
    *
    from lower_munis
    LEFT JOIN (SELECT LOWER(REGEXP_REPLACE(NORMALIZE(nome, NFD), r"\pM", '')) nome_municipio, id_municipio, sigla_uf as sigla_uf1 FROM basedosdados.br_bd_diretorios_brasil. 
    municipio) as mun
    ON lower_munis.nome_mun = mun.nome_municipio AND lower_munis.sigla_uf = mun.sigla_uf1)

SELECT
    SAFE_CAST(data as DATE) data_referencia,
    SAFE_CAST(FORMAT_DATE('%Y-%m-%d', safe.PARSE_DATE('%Y%m%d', data_inscricao))as DATE) AS data_inscricao,
    SAFE_CAST(id_imovel_receita_federal as STRING) id_imovel_receita_federal,
    SAFE_CAST(id_imovel_incra as STRING) id_imovel_incra,
    SAFE_CAST(nome as STRING) nome,
    SAFE_CAST(area as FLOAT64) area,
    SAFE_CAST(cd_rever as STRING) status_sncr,
    SAFE_CAST(status_rever as STRING) tipo_itr,
    SAFE_CAST(situacao as STRING) situacao_imovel,
    SAFE_CAST(endereco as STRING) endereco,
    SAFE_CAST(cep as STRING) cep,
    SAFE_CAST(zona_redefinir as STRING) distrito,
    SAFE_CAST(id_municipio as STRING) id_municipio,
    SAFE_CAST(sigla_uf as STRING) sigla_uf,
    --- esta coluna não é identifica no dicionário nem nomeada nos arquivos
    --- SAFE_CAST(LOWER(status_rever) as STRING) coluna_nao_identificada,
FROM fixed_names AS t
{% if is_incremental() %} 
WHERE data_referencia > (SELECT MAX(data_referencia) FROM {{ this }} )
{% endif %}
