select *
from {schema}.smi_users sut
where 
--sut."smi_int_followers" < 5000
--AND sut."smi_int_followers" > 50
--AND sut."smi_int_friends" < 5000
--AND sut."smi_int_friends" > 50
--AND 
sut."smi_bool_protected" = False
AND (sut."smi_str_lastlookup" IS NULL
OR TO_DATE(sut."smi_str_lastlookup", 'YYYY-MM-DD') < CURRENT_DATE);