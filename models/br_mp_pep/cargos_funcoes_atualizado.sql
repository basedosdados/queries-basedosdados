{{ 
    config(
        schema = 'br_mp_pep', 
        materialized='table',
        partition_by={
            'field': 'ano', 
            'data_type': 'int64', 
            'range': {
            
                "start": 1999,
                "end": 2023,
                "interval": 1
            }
        },
        cluster_by='mes',
        post_hook=[
            'REVOKE `roles/bigquery.dataViewer` ON TABLE {{ this }} FROM "specialGroup:allUsers"',
            'GRANT `roles/bigquery.dataViewer` ON TABLE {{ this }} TO "group:bd-pro@basedosdados.org"'
        ]
    )    
}}

SELECT 
    SAFE_CAST(ano as INT64) as ano,
    SAFE_CAST(mes as INT64) as mes,
    SAFE_CAST(funcao as STRING) as funcao,
    SAFE_CAST(natureza_juridica as STRING) as natureza_juridica,
    SAFE_CAST(orgao_superior as STRING) as orgao_superior,
    SAFE_CAST(escolaridade_servidor as STRING) as escolaridade_servidor,
    SAFE_CAST(orgao as STRING) as orgao,
    SAFE_CAST(regiao as STRING) as regiao,
    SAFE_CAST(sexo as STRING) as sexo,
    SAFE_CAST(nivel_funcao as STRING) as nivel_funcao,
    SAFE_CAST(subnivel_funcao as STRING) as subnivel_funcao,
    SAFE_CAST(sigla_uf as STRING) as sigla_uf,
    SAFE_CAST(faixa_etaria as STRING) as faixa_etaria,
    SAFE_CAST(raca_cor as STRING) as raca_cor,
    SAFE_CAST(cce_e_fce as INT64) as cce_e_fce,
    SAFE_CAST(das_e_correlatas as INT64) as das_e_correlatas
FROM `basedosdados-dev.br_mp_pep_staging.cargos_funcoes`