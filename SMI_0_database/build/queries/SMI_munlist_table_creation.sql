-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_munlist;

create table smi_schema.smi_munlist (
    "location" varchar NOT NULL
);

