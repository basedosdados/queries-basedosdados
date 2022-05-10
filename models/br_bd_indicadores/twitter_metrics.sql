SELECT
SAFE_CAST(upload_ts AS INT64) upload_ts,
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
FROM `basedosdados-dev.br_bd_indicadores_staging.twitter_metrics`
