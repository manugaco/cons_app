-- Script that removes duplicates in the table smi_users:

DELETE FROM {schema}.smi_users suta
    WHERE EXISTS (SELECT 1
                  FROM {schema}.smi_users sutb
                  WHERE sutb."id" = suta."id" AND
                        sutb.ctid < suta.ctid
                 );