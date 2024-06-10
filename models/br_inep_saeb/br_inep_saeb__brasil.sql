{{ config(alias="brasil", schema="br_inep_saeb", materialized="table") }}

select
    safe_cast(ano as int64) ano,
    safe_cast(lower(rede) as string) rede,
    safe_cast(lower(localizacao) as string) localizacao,
    safe_cast(disciplina as string) disciplina,
    safe_cast(
        case
            when serie = "12" then "3" when serie = "13" then "4" else serie
        end as int64
    ) serie,
    safe_cast(media as float64) media,
    safe_cast(nivel_0 as float64) nivel_0,
    safe_cast(nivel_1 as float64) nivel_1,
    safe_cast(nivel_2 as float64) nivel_2,
    safe_cast(nivel_3 as float64) nivel_3,
    safe_cast(nivel_4 as float64) nivel_4,
    safe_cast(nivel_5 as float64) nivel_5,
    safe_cast(nivel_6 as float64) nivel_6,
    safe_cast(nivel_7 as float64) nivel_7,
    safe_cast(nivel_8 as float64) nivel_8,
    safe_cast(nivel_9 as float64) nivel_9,
    safe_cast(nivel_10 as float64) nivel_10
from `basedosdados-staging.br_inep_saeb_staging.brasil` as t
