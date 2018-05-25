CREATE TABLE IF NOT EXISTS user(id INTEGER PRIMARY KEY,
                  name TEXT,
                  lang TEXT,
                  location TEXT);

INSERT INTO user(id, name, lang, location)
SELECT DISTINCT user_id, name, lang, location
FROM tweet;

ALTER TABLE tweet RENAME TO tweet_old;

CREATE TABLE IF NOT EXISTS tweet(id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tweet_id INTEGER,
                        user_id INTEGER,
                        tweet_text TEXT,
                        country_code TEXT,
                        display_url TEXT,
                        created_at TIMESTAMP,                        
                        tweet_sentiment INTEGER,
                        FOREIGN KEY (user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE NO ACTION);

INSERT INTO tweet(tweet_id, user_id, tweet_text, country_code, display_url, created_at, tweet_sentiment)
       select tweet_id, 
              user_id, 
              tweet_text, 
              country_code, 
              display_url, 
              created_at,              
              tweet_sentiment
       from tweet_old;

DROP TABLE tweet_old;

