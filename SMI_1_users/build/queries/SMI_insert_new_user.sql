INSERT INTO {schema}.smi_users_table ("id", "screenName", "followersCount", "friendsCount", "protected", "location", "lang", "ff_lookup")
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT
DO NOTHING;

DELETE FROM {schema}.smi_users_table suta
    WHERE EXISTS (SELECT 1
                  FROM {schema}.smi_users_table sutb
                  WHERE sutb."id" = suta."id" AND
                        sutb.ctid < suta.ctid
                 );