-- Script that creates the database schemas and tables:

drop table if exists smi_schema.smi_initial_users_table;

create table smi_schema.smi_initial_users_table (
	
    "screenName" varchar(255) NOT NULL,
    "followersCount" numeric,
    "follows" numeric,
    "nTweets" numeric,
    "tweetsSince" date,
    "lastTweet" date,
    "category" varchar(255)

);

