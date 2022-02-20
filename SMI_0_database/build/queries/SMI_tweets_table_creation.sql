-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_tweets;

create table smi_schema.smi_tweets (
    "username" varchar NOT NULL,
    "date" timestamp,
    "retweets" numeric,
    "favorites" numeric,
    "text" varchar
);

