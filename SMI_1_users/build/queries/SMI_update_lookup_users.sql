UPDATE {schema}.smi_users_table sut
SET "ff_lookup" = True 
WHERE sut."screenName" = (%s);