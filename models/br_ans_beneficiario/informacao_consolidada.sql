{{ config(
    schema='br_ans_beneficiario',
    materialized='incremental',
    partition_by={
      "field": "ano",
      "data_type": "int64",
      "range": {
        "start": 2014,
        "end": 2023,
        "interval": 1}
    },    
    cluster_by = ["id_municipio", "mes", "sigla_uf"],    
    labels = {'project_id': 'basedosdados'}, 
    pre_hook = "DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    post_hook = [
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter 
                        ON {{this}}
                        GRANT TO ("allUsers")
                        FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter 
                        ON  {{this}}
                        GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org")
                        FILTER USING (DATE_DIFF(CURRENT_DATE(),DATE(CAST(ano AS INT64),CAST(mes AS INT64),1), MONTH) <= 6)']          )
 }}

with ans as (
SELECT
CAST(ano AS INT64) ano,
CAST(mes AS INT64) mes,
CAST(t.sigla_uf AS STRING) sigla_uf,
id_municipio,
CAST(CD_OPERADORA AS STRING) codigo_operadora,
CAST(INITCAP(TRANSLATE(NM_RAZAO_SOCIAL, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) razao_social,
CAST(LPAD(NR_CNPJ,14,'0') AS STRING) cnpj, modalidade_operadora,
CAST(TP_SEXO AS STRING) sexo,
CAST(LOWER(TRANSLATE(DE_FAIXA_ETARIA, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) faixa_etaria,
CAST(LOWER(TRANSLATE(DE_FAIXA_ETARIA_REAJ, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) faixa_etaria_reajuste,
CAST(CD_PLANO AS STRING) codigo_plano,
CAST(TP_VIGENCIA_PLANO AS STRING) tipo_vigencia_plano,
CAST(INITCAP(TRANSLATE(DE_CONTRATACAO_PLANO, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) contratacao_beneficiario,
CAST(INITCAP(TRANSLATE(DE_SEGMENTACAO_PLANO, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) segmentacao_beneficiario,
CAST(DE_ABRG_GEOGRAFICA_PLANO AS STRING) abrangencia_beneficiario,
CAST(INITCAP(TRANSLATE(COBERTURA_ASSIST_PLAN, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) cobertura_assistencia_beneficiario,
CAST(INITCAP(TRANSLATE(TIPO_VINCULO, 'áàâãäéèêëíìîïóòôõöúùûüçÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ', 'aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC')) AS STRING) tipo_vinculo,
CAST(QT_BENEFICIARIO_ATIVO AS INT64) quantidade_beneficiario_ativo,
CAST(QT_BENEFICIARIO_ADERIDO AS INT64) quantidade_beneficiario_aderido,
CAST(QT_BENEFICIARIO_CANCELADO AS INT64) quantidade_beneficiario_cancelado,
CAST(PARSE_DATE('%d/%m/%Y', DT_CARGA) AS DATE) data_carga,
FROM `basedosdados-staging.br_ans_beneficiario_staging.informacao_consolidada` t
join `basedosdados.br_bd_diretorios_brasil.municipio` bd
on t.CD_MUNICIPIO = bd.id_municipio_6)
select *
from ans
{% if is_incremental() %}
where
  data_carga > (SELECT MAX(data_carga) FROM {{ this }}) 
{% endif %}

