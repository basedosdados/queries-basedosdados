{{ config(alias='itr',schema='br_rf_arrecadacao') }}
select
safe_cast(ano as int64) ano,
safe_cast(mes as int64) mes,
safe_cast(nome_uf as string) nome_uf,
safe_cast(regiao_politica as string) regiao_politica,
safe_cast(cidade_uf as string) cidade_uf,
safe_cast(valor_arrecadado as float64) valor_arrecadado,
from `basedosdados-staging.br_rf_arrecadacao_staging.itr` as t

