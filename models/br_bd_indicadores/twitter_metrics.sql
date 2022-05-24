{{
    config(
        materialized='incremental',
        partition_by={
            "field": "upload_day",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


SELECT *
FROM
(SELECT
SAFE_CAST(upload_ts AS INT64) upload_ts,
EXTRACT(DATE FROM TIMESTAMP_MILLIS(upload_ts*1000)) AS upload_day,
SAFE_CAST(id AS STRING) id,
SAFE_CAST(text AS STRING) text,
SAFE_CAST(created_at AS STRING) created_at,
SAFE_CAST(retweet_count AS INT64) retweet_count,
SAFE_CAST(reply_count AS INT64) reply_count,
SAFE_CAST(like_count AS INT64) like_count,
SAFE_CAST(quote_count AS INT64) quote_count,
SAFE_CAST(impression_count AS FLOAT64) impression_count,
SAFE_CAST(user_profile_clicks AS FLOAT64) user_profile_clicks,
SAFE_CAST(url_link_clicks AS FLOAT64) url_link_clicks,
SAFE_CAST(following_count AS INT64) following_count,
SAFE_CAST(followers_count AS INT64) followers_count,
SAFE_CAST(tweet_count AS INT64) tweet_count,
SAFE_CAST(listed_count AS INT64) listed_count
FROM `basedosdados-dev.br_bd_indicadores_staging.twitter_metrics`)
WHERE
    upload_day <= CURRENT_DATE('America/Sao_Paulo')

{% if is_incremental() %}

{% set max_partition = run_query("SELECT gr FROM (SELECT IF(max(upload_day) > CURRENT_DATE('America/Sao_Paulo'), CURRENT_DATE('America/Sao_Paulo'), max(upload_day)) as gr FROM " ~ this ~ ")").columns[0].values()[0] %}

AND
    upload_day > ("{{ max_partition }}")

{% endif %}