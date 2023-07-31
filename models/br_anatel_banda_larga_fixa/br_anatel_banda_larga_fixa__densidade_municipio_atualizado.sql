{{ config(alias='densidade_municipio_atualizado', schema='br_anatel_banda_larga_fixa',
    post_hook=['REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
                'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"']) }}


SELECT

SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
REPLACE(CAST(id_municipio AS STRING), '.0', '') id_municipio,
SAFE_CAST(densidade AS FLOAT64) densidade

FROM basedosdados-staging.br_anatel_banda_larga_fixa_staging.densidade_municipio AS t

