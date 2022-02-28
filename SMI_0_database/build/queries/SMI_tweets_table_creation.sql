-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_tweets;

-- Create tweets table:
create table smi_schema.smi_tweets (
    "smi_str_username" varchar NOT NULL,
    "smi_ts_date" timestamp,
    "smi_str_tweet" varchar
);

