-- Create main schema: 

create schema smi_schema;

-- Create initial users table:

create table smi_schema.smi_initial_users (
	
    "screenName" varchar(255) NOT NULL,
    "followersCount" numeric,
    "follows" numeric,
    "nTweets" numeric,
    "tweetsSince" date,
    "lastTweet" date,
    "category" varchar(255)

);

-- Create users table:

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

-- Create municipalities table:

create table smi_schema.smi_munlist (
    "location" varchar(255) NOT NULL
);

-- Create corpus table:

create table smi_schema.smi_corpus (
    "tweetid" varchar(255) NOT NULL,
    "user" varchar(255) NOT NULL,
    "content" varchar(255),
    "date" varchar(255),
    "lang" varchar(255),
    "sentiment" varchar(255)
);

-- Create tweets table:

create table smi_schema.smi_tweets (
    "username" varchar(255) NOT NULL,
    "date" timestamp,
    "retweets" numeric,
    "favorites" numeric,
    "text" varchar(255)
);