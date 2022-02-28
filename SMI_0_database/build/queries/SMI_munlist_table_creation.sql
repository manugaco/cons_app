-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_munlist;

-- Create municipalities table:
create table smi_schema.smi_munlist (
    "smi_str_location" varchar NOT NULL
);

