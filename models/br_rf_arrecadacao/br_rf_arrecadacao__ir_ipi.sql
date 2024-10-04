{{ config(alias="ir_ipi", schema="br_rf_arrecadacao") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(tributo as string) tributo,
    safe_cast(decendio as string) decendio,
    safe_cast(arrecadacao_bruta as float64) arrecadacao_bruta,
    safe_cast(retificacao as float64) retificacao,
    safe_cast(compensacao as float64) compensacao,
    safe_cast(restituicao as float64) restituicao,
    safe_cast(outros as float64) outros,
    safe_cast(arrecadacao_liquida as float64) arrecadacao_liquida,
from `basedosdados-staging.br_rf_arrecadacao_staging.ir_ipi` as t
