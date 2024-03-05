{{ config(alias="deputado_profissao", schema="br_camara_dados_abertos") }}
select
    safe_cast(id_deputado as int64) id_deputado,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(data)), 'T')[
            offset(0)
        ] as date
    ) data,
    safe_cast(
        split(format_timestamp('%Y-%m-%dT%H:%M:%E*S', timestamp(data)), 'T')[
            offset(1)
        ] as time
    ) horario,
    safe_cast(id_profissao as string) id_profissao,
    safe_cast(titulo as string) titulo,
from `basedosdados-staging.br_camara_dados_abertos_staging.deputado_profissao` as t
