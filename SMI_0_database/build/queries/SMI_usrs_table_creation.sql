-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_users;

create table smi_schema.smi_users (
    "id" varchar(255) NOT NULL,
    "screenName" varchar(255) NOT NULL,
    "followersCount" numeric,
    "friendsCount" numeric,
    "protected" boolean,
    "location" varchar(255),
    "lang" varchar(255),
    "ff_lookup" boolean
);
