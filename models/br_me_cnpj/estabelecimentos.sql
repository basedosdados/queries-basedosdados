{{
  config(
    schema='br_me_cnpj',
    materialized='incremental',
    partition_by={
      "field": "data",
      "data_type": "date",
    },
    cluster_by='sigla_uf',
    pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    post_hook=['CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(data), MONTH) > 6 OR  DATE_DIFF(DATE(2023,5,1),DATE(data), MONTH) > 0)',
              'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (EXTRACT(YEAR from data) = EXTRACT(YEAR from  CURRENT_DATE()))' ]) 
}}
WITH cnpj_estabelecimentos AS 
(SELECT 
  SAFE_CAST(data AS DATE) data,
  SAFE_CAST(lpad(cnpj,14,"0") AS STRING) cnpj,
  SAFE_CAST(lpad(cnpj_basico, 8, '0') AS STRING) cnpj_basico,
  SAFE_CAST(lpad(cnpj_ordem, 4, '0') AS STRING) cnpj_ordem,
  SAFE_CAST(lpad(cnpj_dv, 2, '0') AS STRING) cnpj_dv,
  SAFE_CAST(identificador_matriz_filial AS STRING) identificador_matriz_filial,
  SAFE_CAST(nome_fantasia AS STRING) nome_fantasia,
  SAFE_CAST(CAST(situacao_cadastral AS INT64) AS STRING) situacao_cadastral,
  SAFE_CAST(data_situacao_cadastral AS DATE) data_situacao_cadastral,
  SAFE_CAST(motivo_situacao_cadastral AS STRING) motivo_situacao_cadastral,
  SAFE_CAST(nome_cidade_exterior AS STRING) nome_cidade_exterior,
  SAFE_CAST(CAST(id_pais AS INT64) AS STRING) id_pais,
  SAFE_CAST(data_inicio_atividade AS DATE) data_inicio_atividade,
  SAFE_CAST(cnae_fiscal_principal AS STRING) cnae_fiscal_principal,
  SAFE_CAST(cnae_fiscal_secundaria AS STRING) cnae_fiscal_secundaria,
  SAFE_CAST(a.sigla_uf AS STRING) sigla_uf,
  SAFE_CAST(b.id_municipio AS STRING) id_municipio,
  SAFE_CAST(SAFE_CAST(a.id_municipio_rf AS NUMERIC)AS STRING) id_municipio_rf,
  SAFE_CAST(tipo_logradouro AS STRING) tipo_logradouro,
  SAFE_CAST(logradouro AS STRING) logradouro,
  SAFE_CAST(numero AS STRING) numero,
  SAFE_CAST(complemento AS STRING) complemento,
  SAFE_CAST(bairro AS STRING) bairro,
  SAFE_CAST(REPLACE (cep,".0","") AS STRING) cep,
  SAFE_CAST(ddd_1 AS STRING) ddd_1,
  SAFE_CAST(telefone_1 AS STRING) telefone_1,
  SAFE_CAST(ddd_2 AS STRING) ddd_2,
  SAFE_CAST(telefone_2 AS STRING) telefone_2,
  SAFE_CAST(ddd_fax AS STRING) ddd_fax,
  SAFE_CAST(fax AS STRING) fax,
  SAFE_CAST(LOWER(email) AS STRING) email,
  SAFE_CAST(situacao_especial AS STRING) situacao_especial,
  SAFE_CAST(data_situacao_especial AS DATE) data_situacao_especial
FROM basedosdados-staging.br_me_cnpj_staging.estabelecimentos a
LEFT JOIN basedosdados.br_bd_diretorios_brasil.municipio b
    ON SAFE_CAST(SAFE_CAST(a.id_municipio_rf AS NUMERIC)AS STRING)  = b.id_municipio_rf)
SELECT * FROM cnpj_estabelecimentos
{% if is_incremental() %} 
WHERE data > (SELECT MAX(data) FROM {{ this }} )
{% endif %}