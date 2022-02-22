# Consumption app (cons_app)

This app has the aim of deploying the social media consumption index into a server and visualize it.

## Technologies used

- Python and postgresSQL.
- Docker.
- json backups format.

## Repository structure

### SMI_0_database folder

Docker container service that mounts and fills a postgres database.
It creates all the database structure, schema, tables and variables.

More explanation about the process is explained in the readme of the SMI_0_database folder.

### SMI_1_users folder

Docker container service that retrieves friends and followers from a users list.
Given a user list, it retrieves all its friends and followers and it stores them in the database.

### SMI_2_tweets folder

Docker container service that retrieves all the tweets from a user list.
Given a period range, it retrieves all the tweets and stores them in the database.

### SMI_3_model folder

Docker container service to train a classification supervised model.
Given a labeled dataset, it executes a machine learning modelling pipeline.

### SMI_4_visual folder

(to think about...)
