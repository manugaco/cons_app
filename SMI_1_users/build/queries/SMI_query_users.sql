select *
from {schema}.smi_users sut
where 
sut."smi_int_followers" < 30
AND sut."smi_int_followers" > 5
AND sut."smi_int_friends" < 30
AND sut."smi_int_friends" > 5
AND 
sut."smi_bool_protected" = False
AND (sut."smi_str_lastlookup" IS NULL
OR TO_DATE(sut."smi_str_lastlookup", 'YYYY-MM-DD') < CURRENT_DATE);