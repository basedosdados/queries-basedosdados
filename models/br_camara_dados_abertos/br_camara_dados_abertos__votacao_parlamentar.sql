{{
    config(
        alias="votacao_parlamentar",
        schema="br_camara_dados_abertos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2022, "interval": 1},
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
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(data_hora)), 'T')[
            offset(0)
        ] as date
    ) data,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(data_hora)), 'T')[
            offset(1)
        ] as time
    ) horario,
    safe_cast(voto as string) voto,
    safe_cast(replace(id_deputado, ".0", "") as string) id_deputado,
    safe_cast(nome as string) nome,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_legislatura as string) id_legislatura
from `basedosdados-staging.br_camara_dados_abertos_staging.votacao_parlamentar` as t
