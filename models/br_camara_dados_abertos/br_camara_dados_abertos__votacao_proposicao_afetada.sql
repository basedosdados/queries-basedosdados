{{
    config(
        alias="votacao_proposicao_afetada",
        schema="br_camara_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2023, "interval": 1},
        },
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY allusers_filter ON {{this}} GRANT TO ("allUsers") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), week) > 6)',
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON {{this}} GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (DATE_DIFF(DATE("{{ run_started_at.strftime("%Y-%m-%d") }}"), DATE(data), week) <= 6)',
        ],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_votacao as string) id_votacao,
    safe_cast(data as date) data,
    safe_cast(descricao as string) descricao,
    safe_cast(id_proposicao as string) id_proposicao,
    safe_cast(replace(ano_proposicao, ".0", "") as int64) ano_proposicao,
    safe_cast(titulo as string) titulo,
    safe_cast(ementa as string) ementa,
    safe_cast(codigo_tipo as string) codigo_tipo,
    safe_cast(sigla_tipo as string) sigla_tipo,
    safe_cast(replace(numero, ".0", "") as string) numero,
from
    basedosdados
    - staging.br_camara_dados_abertos_staging.votacao_proposicao_afetada as t
