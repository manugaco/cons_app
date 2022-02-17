UPDATE {schema}.smi_users sut
SET "ff_lookup" = True 
WHERE sut."screenName" = (%s);