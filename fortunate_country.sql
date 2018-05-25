select name, lang, location, 'best' as sentiment 
from tweet t1
inner join (select max([tweet_sentiment]) as tweet_sentiment
             from tweet) t2 on t1.tweet_sentiment=t2.tweet_sentiment
inner join user t3 on t1.[user_id]=t3.id
where location is not null
union all
select name, lang, location, 'worst' as sentiment
from tweet t1
inner join (select min([tweet_sentiment]) as tweet_sentiment
             from tweet) t2 on t1.tweet_sentiment=t2.tweet_sentiment
inner join user t3 on t1.[user_id]=t3.id
where location is not null;


