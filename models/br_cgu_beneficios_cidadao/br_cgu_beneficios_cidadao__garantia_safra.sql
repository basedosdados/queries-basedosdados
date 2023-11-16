{{
  config(
    alias='garantia_safra',    
    schema='br_cgu_beneficios_cidadao',
    materialized='incremental',
     partition_by={
      "field": "ano_referencia",
      "data_type": "int64",
      "range": {
        "start": 2013,
        "end": 2024,
        "interval": 1}
    },
    cluster_by = ["mes_referencia", "sigla_uf"] ,
    post_hook = [
        'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                    ON {{this}}
                    GRANT TO ("allUsers")
                    FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano_referencia AS INT64),CAST(mes_referencia AS INT64),1), MONTH) > 6)',
        'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                    ON  {{this}}
                    GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                    FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"),DATE(CAST(ano_referencia AS INT64),CAST(mes_referencia AS INT64),1), MONTH) <= 6)']       )
}}
with garantia_safra as (
SELECT 
SAFE_CAST(SUBSTR(mes_referencia, 1, 4) AS INT64) ano_referencia,
SAFE_CAST(SUBSTR(mes_referencia, 5, 2) AS INT64) mes_referencia,
SAFE_CAST(PARSE_DATE('%Y%m',mes_referencia) AS DATE) data,
t2.id_municipio,
SAFE_CAST(t1.sigla_uf AS STRING) sigla_uf,
SAFE_CAST(nis AS STRING) nis_favorecido,
SAFE_CAST(t1.nome AS STRING) nome_favorecido,
SAFE_CAST(valor AS FLOAT64) valor_parcela,
FROM `basedosdados-staging.br_cgu_beneficios_cidadao_staging.garantia_safra` t1
left join `basedosdados.br_bd_diretorios_brasil.municipio` t2
on SAFE_CAST(t1.id_municipio_siafi AS INT64) = SAFE_CAST(t2.id_municipio_rf AS INT64))
select * except(data) from garantia_safra
{% if is_incremental() %} 
WHERE data > (SELECT MAX(data) FROM {{ this }} )
{% endif %}