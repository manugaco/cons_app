INSERT INTO {schema}.smi_date_tweets 
    ("smi_str_username", "smi_str_datetweets")
VALUES 
    (%s, %s)
ON CONFLICT 
    ("smi_str_username") 
DO 
    UPDATE SET 
        smi_str_datetweets = excluded."smi_str_datetweets";
