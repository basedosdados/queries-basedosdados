select *
from {{ ref('metricas_tweets_dbt_model') }}