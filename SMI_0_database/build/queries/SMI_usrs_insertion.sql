INSERT INTO 
    {schema}.smi_users 
        ("smi_str_userid", 
        "smi_str_username",
        "smi_int_followers", 
        "smi_int_friends", 
        "smi_bool_protected", 
        "smi_str_location", 
        "smi_str_lang", 
        "smi_str_lastlookup")
VALUES 
    (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT 
    ("smi_str_username")
DO NOTHING;