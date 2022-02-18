-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_corpus;

create table smi_schema.smi_corpus (
    "tweetid" varchar(255) NOT NULL,
    "user" varchar(255) NOT NULL,
    "content" varchar(255),
    "date" varchar(255),
    "lang" varchar(255),
    "sentiment" varchar(255)
);

