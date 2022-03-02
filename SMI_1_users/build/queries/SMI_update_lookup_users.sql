UPDATE {schema}.smi_users sut
SET "smi_str_lastlookup" = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
WHERE sut."smi_str_username" = (%s);