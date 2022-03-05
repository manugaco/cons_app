-- Insert new users into DB if does not exists:

INSERT INTO {schema}.smi_date_tweets 
    ("smi_str_username", "smi_str_datetweets")
VALUES 
    (%s, %s)
ON CONFLICT 
    ("smi_str_username")
DO NOTHING;

-- Drop duplicated users:

DELETE FROM {schema}.smi_date_tweets suta
    WHERE EXISTS (SELECT 1
                  FROM {schema}.smi_date_tweets sutb
                  WHERE sutb."smi_str_username" = suta."smi_str_username" AND
                        sutb.ctid < suta.ctid
                 );