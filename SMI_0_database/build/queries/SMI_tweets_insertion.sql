INSERT INTO {schema}.smi_tweets ("smi_str_username", 
                                 "smi_ts_date",
                                 "smi_str_tweet")
VALUES (%s, %s, %s)
ON CONFLICT ("smi_str_username")
DO NOTHING;