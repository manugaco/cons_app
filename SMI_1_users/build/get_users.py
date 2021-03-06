import pandas as pd
import numpy as np
import random as rd
import os
import json
import logging
from tweepy import API, OAuthHandler
import psycopg2
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)
register_adapter(np.bool_, AsIs)

import warnings
warnings.filterwarnings("ignore")

from utils import *

# GLOBAL Params:

with open('../config/global.json') as config_file:
    gb_config = json.load(config_file)

ini_users_dict = gb_config['ini_users_dict']
users_dict = gb_config['users_dict']
tw_acc = gb_config['tw_acc']

# APP Params:

with open('../config/get_users.config') as config_file:
    app_config = json.load(config_file)

queries_path = app_config['queries_path']
logs_path = app_config['logs_path']
temp_data_path = app_config['temp_data_path']
nusers_sample = app_config['nusers_sample']
app_name = app_config['app_name']

# Check and create directories:
if not os.path.isdir(logs_path):
    os.makedirs(logs_path)

if not os.path.isdir(logs_path + app_name):
    os.makedirs(logs_path + app_name)

if not os.path.isdir(temp_data_path):
    os.makedirs(temp_data_path)

# Twitter API credentials:

consumer_key = tw_acc['CONSUMER_KEY'] 
consumer_secret = tw_acc['CONSUMER_SECRET'] 
access_token = tw_acc['ACCESS_TOKEN']
access_secret = tw_acc['ACCESS_SECRET']

# Twitter API connection:

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = API(auth, wait_on_rate_limit=True)

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
    upipe = UsersPipeline(queries_path, conn, schema, 
                          api, temp_data_path, api_logger)

    # Get municipalities from DB:
    db_munlist = upipe.fetchall_SQL(queries_path + 'SMI_munlist_query.sql')
    munlist = pd.DataFrame(db_munlist, columns = ['location'])['location'].tolist()
    db_munlist = [upipe.text_clean_loc(mun.lower().replace(',', '')) for mun in munlist]

except (Exception, psycopg2.DatabaseError) as error:
    logging.exception(error)

# Infinte retrieval of users:
while True:
    try:
        logging.info('Twetter API job: Starting iteration.')
        # Get users table from database:
        with open(queries_path + 'SMI_query_users.sql', 'r') as f:
            query = f.read().format(schema=schema)
            cur.execute(query)
            db_users = pd.DataFrame(cur.fetchall(), columns = list(users_dict.keys()))
            db_users = db_users.astype({"smi_str_userid": object})
            users_ls = db_users["smi_str_username"].to_list()

        # Sample users on each iteration:
        userls = rd.sample(users_ls, nusers_sample)

        # Get users loop:
        for user in userls:
            upipe.get_and_insert_new_users(user, db_munlist, api)

    except Exception as error:
        logging.exception(error)
