select *
from {schema}.smi_users sut
where 
--remove once tested
sut."followersCount" < 100
AND sut."followersCount" > 20
AND sut."friendsCount" < 100
AND sut."friendsCount" > 20
AND 
--remove once tested
sut."protected" = False
AND sut."ff_lookup" = False;