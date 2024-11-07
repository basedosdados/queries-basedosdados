{{ config(alias="questionario_socioeconomico_2023", schema="br_inep_enem") }}

select
    safe_cast(id_inscricao as string) id_inscricao,
    safe_cast(q001 as string) q001,
    safe_cast(q002 as string) q002,
    safe_cast(q003 as string) q003,
    safe_cast(q004 as string) q004,
    safe_cast(q005 as string) q005,
    safe_cast(q006 as string) q006,
    safe_cast(q007 as string) q007,
    safe_cast(q008 as string) q008,
    safe_cast(q009 as string) q009,
    safe_cast(q010 as string) q010,
    safe_cast(q011 as string) q011,
    safe_cast(q012 as string) q012,
    safe_cast(q013 as string) q013,
    safe_cast(q014 as string) q014,
    safe_cast(q015 as string) q015,
    safe_cast(q016 as string) q016,
    safe_cast(q017 as string) q017,
    safe_cast(q018 as string) q018,
    safe_cast(q019 as string) q019,
    safe_cast(q020 as string) q020,
    safe_cast(q021 as string) q021,
    safe_cast(q022 as string) q022,
    safe_cast(q023 as string) q023,
    safe_cast(q024 as string) q024,
    safe_cast(q025 as string) q025
from `basedosdados-staging.br_inep_enem_staging.questionario_socioeconomico_2023` as t