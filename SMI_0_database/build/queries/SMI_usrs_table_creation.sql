-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_users;

-- Create users table:
create table smi_schema.smi_users (
    "smi_str_userid" varchar NOT NULL,
    "smi_str_username" varchar NOT NULL,
    "smi_int_followers" numeric,
    "smi_int_friends" numeric,
    "smi_bool_protected" boolean,
    "smi_str_location" varchar,
    "smi_str_lang" varchar,
    "smi_str_lastlookup" varchar
);
