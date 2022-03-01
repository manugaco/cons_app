insert into 
	smi_schema.smi_date_tweets 
	(
		smi_str_username, 
		smi_str_datetweets
	)
select 
	foo.smi_str_username, 
	string_agg(to_char(foo."smi_ts_date", 'YYYY-MM-DD'), ', ') as smi_str_datetweets
from 
	(
	select 
		su.smi_str_username, 
		date(st."smi_ts_date") as smi_ts_date
	from smi_schema.smi_users su 
	left join smi_schema.smi_tweets st 
	on su.smi_str_username = st.smi_str_username
	group by 1, 2
	order by 1, 2) foo
group by 1;