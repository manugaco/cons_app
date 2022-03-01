-- Create main schema: 
create schema smi_schema;

-- Create users table:
create table smi_schema.smi_users (
    "smi_str_userid" varchar NOT NULL,
    "smi_str_username" varchar NOT NULL,
    "smi_int_followers" numeric,
    "smi_int_friends" numeric,
    "smi_bool_protected" boolean,
    "smi_str_location" varchar,
    "smi_str_lang" varchar,
    "smi_str_lastlookup" varchar,
    primary key ("smi_str_username")
);

-- Create municipalities table:
create table smi_schema.smi_munlist (
    "smi_str_location" varchar NOT NULL
);

-- Create corpus table:
create table smi_schema.smi_corpus (
    "smi_str_username" varchar NOT NULL,
    "smi_ts_date" varchar,
    "smi_str_tweet" varchar,
    "smi_str_sentiment" varchar
);

-- Create tweets table:
create table smi_schema.smi_tweets (
    "smi_str_username" varchar NOT NULL,
    "smi_ts_date" timestamp,
    "smi_str_tweet" varchar
);
-- Create dates of tweets retrieved on 

create table smi_schema.smi_date_tweets (
    "smi_str_username" varchar NOT NULL,
    "smi_str_datetweets" varchar,
    primary key("smi_str_username")
);