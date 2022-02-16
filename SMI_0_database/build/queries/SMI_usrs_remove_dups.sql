-- Script that removes duplicates in the table smi_users_table:

DELETE FROM {schema}.smi_users_table suta
    WHERE EXISTS (SELECT 1
                  FROM {schema}.smi_users_table sutb
                  WHERE sutb."id" = suta."id" AND
                        sutb.ctid < suta.ctid
                 );