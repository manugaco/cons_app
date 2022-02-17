## cons_app

This app has the aim of deploy the social media consumption index into a server and visualize it.

###Technologies used:

 - Python and postgresSQL.
 - Docker.

###Repository structure:

* SMI_0_database folder:

 Docker container service that mounts and fills a postgres database.

  - It creates all the database structure, schema, tables and variables.
  - It fills each table with information:
    - Initial users table: It will be filled whether the user has a backup or not, in the last case, the initial users are retrieved from a web url (it can also be customized).
    - Users table: It will be filled if and only if a backup is available.
    - Municipalities: In order to filter the origin of the users via location, a list of municipalities (or any state decomposition) must be provided.
    - Corpus: In order to train a supervised model to classify future tweets, a labeled corpus linguistic must be provided.
    - Tweets: It will be filled if and only if a backup is available.

* SMI_1_users folder:



It is implemented by using python and postgresSQL for coding 
