INSERT INTO {schema}.smi_users (username, date_tweets)
VALUES (%s, %s)
ON CONFLICT (username) DO UPDATE SET date_tweets = smi_date_tweets.date_tweets || ', ' || excluded.date_tweets;
