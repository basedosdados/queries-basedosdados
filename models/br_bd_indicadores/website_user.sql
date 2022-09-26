SELECT 
SAFE_CAST(reference_date AS DATE) reference_date,
SAFE_CAST(users_1_day AS INT64) users_1_day,
SAFE_CAST(users_7_days AS INT64) users_7_days,
SAFE_CAST(users_14_days AS INT64) users_14_days,
SAFE_CAST(users_28_days AS INT64) users_28_days,
SAFE_CAST(users_30_days AS INT64) users_30_days,
SAFE_CAST(new_users AS INT64) new_users
FROM basedosdados-dev.br_bd_indicadores_staging.website_user AS t