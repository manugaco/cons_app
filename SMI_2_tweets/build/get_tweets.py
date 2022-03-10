import pandas as pd
import random
import os
import json
import time
import logging
from datetime import datetime as dt
import spacy
nlp = spacy.load('es_core_news_sm')

import warnings
warnings.filterwarnings('ignore')

from utils import *

# APP Params:

with open('../config/get_tweets.config') as config_file:
    app_config = json.load(config_file)

queries_path = app_config['queries_path']
logs_path = app_config['logs_path']
temp_data_path = app_config['temp_data_path']
app_name = app_config['app_name']

# Check and create directories:
if not os.path.isdir(logs_path):
    os.makedirs(logs_path)

if not os.path.isdir(logs_path + app_name):
    os.makedirs(logs_path + app_name)

if not os.path.isdir(temp_data_path):
    os.makedirs(temp_data_path)

# Logger initialization:
api_logger = logging.getLogger(__file__)
file_name = dt.today().strftime('%y%m%d') + '_' + app_name + '.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_path, app_name, file_name)),
        logging.StreamHandler()
    ])

try:

    api_logger.info('Database jobs: PostgresSQL connection setup.')

    # Get db config:

    with open('../config/postgres.config') as config_file:
        db_config = json.load(config_file)

    # Local database deployment
    conn = psycopg2.connect(
                            dbname=db_config['db_name'],
                            user=db_config['db_user'],
                            host=db_config['db_host'],
                            port=db_config['db_port'],
                            password=db_config['db_password'],
                            options=db_config['db_options']
                            )
    conn.autocommit = True
    cur = conn.cursor()
    schema = db_config['db_schema']

    # Users pipeline class instance:
    tpipe = TweetsPipeline(queries_path, conn, schema, api_logger)

    api_logger.info('Data jobs: Load auxiliar data from files.')

    ##Load stopwords:
    stops = ['/db_stopwords_spanish_1.txt', '/db_stopwords_spanish_2.txt']
    stopw = tpipe.get_stops(temp_data_path + 'utils', stops)
    ##Load ecolist:
    files = ['/ecofilter.xlsx']
    ecolist = tpipe.get_ecofilter(temp_data_path + 'utils', files)

except (Exception, psycopg2.DatabaseError) as error:
    logging.exception(error)

# Process initialization:

while True:
    try:

        ## Select user randomly from date tweets table (SQL query) "user":
        api_logger.info('Database job: Select user randomly from DB.')
        user = tpipe.fetchall_SQL(queries_path + 'SMI_get_random_user.sql')[0]

        ## Remove comma on the head of the vectorized version of the column "smi_str_datetweets" of the user and call it "user_date_tweets"
        ## Create object with username called user_screename:
        user = list(user)
        if len(user[1]) > 0:
            if user[1].split()[0] == ',':
                user[1] = ', '.join(user[1].split(', ')[1:])

        user_screename = user[0]
        user_date_tweets = user[1]

        ## Create vector of dates [2012-01-01, today] called "all_dates":
        all_dates = [str(date.date()) for date in pd.date_range('2012-01-01', pd.to_datetime("today"), freq='D')]

        ## Create vector of disjoint dates between "all_dates" and "user_date_tweets" and call it "dates_to_scrap":
        dates_to_scrap = list(set(all_dates) - set(user_date_tweets))

        ## Select randomly one of "dates_to_scrap":
        ini_date = random.choice(dates_to_scrap)

        ## Create end_date as initial_date + 1 day:
        end_date = (pd.to_datetime(ini_date) + pd.DateOffset(days=1)).date().strftime('%Y-%m-%d')

        ## Scrap the tweets of the user on that dates range:
        api_logger.info('Scrapping job: Retrieving tweets from user.')
        df_tweets = tpipe.get_tweets(user_screename, ini_date, end_date, stopw, ecolist)
        api_logger.info('Scrapping job: Number of scrapped tweets: ' + str(df_tweets.shape[0]))
        
        if df_tweets.shape[0] > 0:

            ## Insert the tweets in the tweets table (df_to_postgres) if there are tweets, otherwise do nothing
            api_logger.info('Database job: Inserting scrapped tweets on DB.')
            tpipe.df_to_postgres(df_tweets, 'smi_tweets')
            api_logger.info('Database job: Scrapped tweets inserted on DB.')

            ## Execute SQL query to remove duplicated entries on the tweets table:
            api_logger.info('Database job: Removing duplicated entries.')
            tpipe.query_SQL(queries_path + 'SMI_remove_dup_tweets.sql')
            api_logger.info('Database job: Duplicated entries removed.')

            ## Update new retrieval date on smi_date_tweets table on DB
            api_logger.info('Database job: Inserting new scrapped date tweets into DB.')
            user_to_insert = user.copy()
            user_to_insert[1] = ', '.join(user_to_insert[1].split(', ') + [ini_date])
            tpipe.insert_datetweets_into_db(queries_path + 'SMI_insert_date_tweets.sql', tuple(user_to_insert))
            api_logger.info('Database job: Scrapped date tweets inserted into DB.')

        else:

            api_logger.info('Database job: There are no tweets to insert on DB.')
            
        ## SLEEP n seconds, choose n randomly:
        sleep_time = random.randint(15, 30)
        api_logger.info('Sleeping sistem for: ' + str(sleep_time) + ' seconds.')
        time.sleep(sleep_time)

    except Exception as error:
        logging.exception(error)