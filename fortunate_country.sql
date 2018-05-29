   SELECT t1.name,
          t1.lang,
          t1.location,
          t1.tweet_sentiment as sentiment,
          coalesce(t2.sentiment_desc, t3.sentiment_desc) as sentiment_desc
     FROM tweet t1
LEFT JOIN (select max([tweet_sentiment]) as tweet_sentiment, 'best' as sentiment_desc
             from tweet) t2 on t1.tweet_sentiment = t2.tweet_sentiment
LEFT JOIN (select min([tweet_sentiment]) as tweet_sentiment, 'worst' as sentiment_desc
             from tweet) t3 on t1.tweet_sentiment = t3.tweet_sentiment
    WHERE location IS NOT NULL
      AND (t2.tweet_sentiment
               OR t3.tweet_sentiment)
 ORDER BY t1.tweet_sentiment