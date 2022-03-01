-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_date_tweets;

-- Create corpus table:
create table smi_schema.smi_date_tweets (
    "smi_str_username" varchar NOT NULL,
    "smi_str_datetweets" varchar,
    primary key("smi_str_username")
);

