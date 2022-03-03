-- Script that removes duplicates in the table smi_users:

DELETE FROM {schema}.smi_tweets suta
    WHERE EXISTS (SELECT 1
                  FROM {schema}.smi_tweets sutb
                  WHERE sutb."smi_str_username" = suta."smi_str_username" 
                  AND sutb."smi_ts_date" = suta."smi_ts_date"
                  AND sutb."smi_str_tweet" = suta."smi_str_tweet"
                  AND sutb.ctid < suta.ctid
                 );