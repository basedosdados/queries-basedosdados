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
    safe_cast(q025 as string) q025,
    safe_cast(q026 as string) q026,
    safe_cast(q027 as string) q027,
    safe_cast(q028 as string) q028,
    safe_cast(q029 as string) q029,
    safe_cast(q030 as string) q030,
    safe_cast(q031 as string) q031,
    safe_cast(q032 as string) q032,
    safe_cast(q033 as string) q033,
    safe_cast(q034 as string) q034,
    safe_cast(q035 as string) q035,
    safe_cast(q036 as string) q036,
    safe_cast(q037 as string) q037,
    safe_cast(q038 as string) q038,
    safe_cast(q039 as string) q039,
    safe_cast(q040 as string) q040,
    safe_cast(q041 as string) q041,
    safe_cast(q042 as string) q042,
    safe_cast(q043 as string) q043,
    safe_cast(q044 as string) q044,
    safe_cast(q045 as string) q045,
    safe_cast(q046 as string) q046,
    safe_cast(q047 as string) q047,
    safe_cast(q048 as string) q048,
    safe_cast(q049 as string) q049,
    safe_cast(q050 as string) q050
from
    {{ set_datalake_project("br_inep_enem_staging.questionario_socioeconomico_2015") }}
    as t
