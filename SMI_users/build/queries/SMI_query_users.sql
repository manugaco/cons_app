select *
from {schema}.smi_users_table sut
where sut."followersCount" < 20
AND sut."followersCount" > 10
AND sut."friendsCount" < 20
AND sut."friendsCount" > 10
AND sut."protected" = False
AND sut."ff_lookup" = False;