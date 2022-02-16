# SMI_database:

Consumption app database creation repository.

Structure:
  - build:
    queries: SQL queries to create and sanity check the initial database.
    main.py --> aplication
      - config.py --> parameter configurations
      - utils.py --> data and model pipeline; classes and functions
  - config:
    - db.config --> postgres sql database configuration
To be done:
  - Postgress and python docker integration.
  - Productivization and app integration.
