-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_corpus;

-- Create corpus table:
create table smi_schema.smi_corpus (
    "smi_str_username" varchar NOT NULL,
    "smi_str_date" varchar,
    "smi_str_tweet" varchar,
    "smi_str_sentiment" varchar
);

