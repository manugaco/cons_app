SELECT 
    sdt."smi_str_username", 
    sdt."smi_str_datetweets" 
FROM 
    {schema}.smi_date_tweets sdt
ORDER BY 
    random() 
LIMIT 1;
