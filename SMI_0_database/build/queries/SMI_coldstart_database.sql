-- Create main schema: 

create schema smi_schema;

-- Create initial users table:

create table smi_schema.smi_initial_users (
	
    "screenName" varchar NOT NULL,
    "followersCount" numeric,
    "follows" numeric,
    "nTweets" numeric,
    "tweetsSince" date,
    "lastTweet" date,
    "category" varchar

);

-- Create users table:

create table smi_schema.smi_users (
    "id" varchar NOT NULL,
    "screenName" varchar NOT NULL,
    "followersCount" numeric,
    "friendsCount" numeric,
    "protected" boolean,
    "location" varchar,
    "lang" varchar,
    "ff_lookup" boolean
);

-- Create municipalities table:

create table smi_schema.smi_munlist (
    "location" varchar NOT NULL
);

-- Create corpus table:

create table smi_schema.smi_corpus (
    "tweetid" varchar NOT NULL,
    "user" varchar NOT NULL,
    "content" varchar,
    "date" varchar,
    "lang" varchar,
    "sentiment" varchar
);

-- Create tweets table:

create table smi_schema.smi_tweets (
    "username" varchar NOT NULL,
    "date" timestamp,
    "text" varchar
);