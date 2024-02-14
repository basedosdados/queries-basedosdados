{{
    config(
        alias="votacao_microdados",
        schema="br_camara_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1935, "end": 2023, "interval": 1},
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
    time(timestamp(horario)) as horario,
    safe_cast(id_orgao as string) id_orgao,
    safe_cast(sigla_orgao as string) sigla_orgao,
    safe_cast(id_evento as string) id_evento,
    safe_cast(replace(aprovacao, ".0", "") as int64) aprovacao,
    safe_cast(voto_sim as int64) voto_sim,
    safe_cast(voto_nao as int64) voto_nao,
    safe_cast(voto_outro as int64) voto_outro,
    safe_cast(descricao as string) descricao,
    safe_cast(data_hora_ultima_votacao as datetime) data_hora_ultima_votacao,
    safe_cast(descricao_ultima_votacao as string) descricao_ultima_votacao,
    safe_cast(data_hora_ultima_proposicao as datetime) data_hora_ultima_proposicao,
    safe_cast(descricao_ultima_proposicao as string) descricao_ultima_proposicao,
    safe_cast(id_ultima_proposicao as string) id_ultima_proposicao,
from `basedosdados-staging.br_camara_dados_abertos_staging.votacao_microdados ` as t
