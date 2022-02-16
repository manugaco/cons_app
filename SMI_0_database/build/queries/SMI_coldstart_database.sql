create schema smi_schema;
create table smi_schema.smi_initial_users_table (
	
    "screenName" varchar(255) NOT NULL,
    "followersCount" numeric,
    "follows" numeric,
    "nTweets" numeric,
    "tweetsSince" date,
    "lastTweet" date,
    "category" varchar(255)

);

create table smi_schema.smi_users_table (
    "id" varchar(255) NOT NULL,
    "screenName" varchar(255) NOT NULL,
    "followersCount" numeric,
    "friendsCount" numeric,
    "protected" boolean,
    "location" varchar(255),
    "lang" varchar(255),
    "ff_lookup" boolean
);
