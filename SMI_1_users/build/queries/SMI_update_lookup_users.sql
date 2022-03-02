UPDATE {schema}.smi_users sut
SET "smi_str_lastlookup" = TO_CHAR(DATE(CURRENT_DATE + 1 * INTERVAL '1 MONTH'), 'YYYY-MM-DD')
WHERE sut."smi_str_username" = (%s);