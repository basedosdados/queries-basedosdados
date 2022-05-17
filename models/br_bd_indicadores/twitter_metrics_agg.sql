SELECT
  EXTRACT(DATE FROM TIMESTAMP_MILLIS(upload_ts*1000)) AS upload_day,
  SUM(retweet_count) AS retweets,
  SUM(reply_count) AS replies,
  SUM(like_count) AS likes,
  SUM(quote_count) AS quote_tweets,
  SUM(impression_count) AS impressions,
  SUM(user_profile_clicks) AS profile_clicks,
  SUM(url_link_clicks) AS links_clicks,
  ANY_VALUE(following_count) AS followings,
  ANY_VALUE(followers_count) AS followers,
  ANY_VALUE(tweet_count) AS tweets,
  ANY_VALUE(listed_count) AS listed
FROM `basedosdados-dev.br_bd_indicadores.twitter_metrics`
GROUP BY upload_day
ORDER BY upload_day