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
from cons_app.SMI_commons.smi_commons import *

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
db_users_bkp = app_config['db_users_bkp']
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

    ##Decide whether the users to start with for the retrieval 
    # are the initial or the backup userss
    # Get users table from database:
    with open(queries_path + 'SMI_query_users.sql', 'r') as f:
        query = f.read().format(schema=schema)
        cur.execute(query)
        db_users = pd.DataFrame(cur.fetchall(), columns = list(users_dict.keys()))
        db_users = db_users.astype({"id": object})

    if db_users.shape[0] == 0:
        # Get initial users table from database:
        with open(queries_path + 'SMI_query_ini_users.sql', 'r') as f:
            query = f.read().format(schema=schema)
            cur.execute(query)
            db_ini_users = pd.DataFrame(cur.fetchall(), columns = list(ini_users_dict.keys()))

        users_ls = db_ini_users["screenName"].to_list()
    else:
        users_ls = db_users["screenName"].to_list()

    # Get munlist:
    with open(temp_data_path + 'municipalities/db_munlist.json') as config_file:
        db_munlist = json.load(config_file)

    # Users pipeline class instance:
    upipe = UsersPipeline(queries_path, conn, schema, api, temp_data_path, api_logger)

    # Treat munlist:
    db_munlist = [upipe.text_clean_loc(name.lower().replace(',', '')) for name in db_munlist]
    
    # Sample users:
    userls = rd.sample(users_ls, nusers_sample)

    # Get several users loop:
    for user in userls:
        upipe.get_and_insert_new_users(user, db_munlist, api)

except (Exception, psycopg2.DatabaseError) as error:
    logging.exception(error)
    finally_exit = 1

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    logging.info('Database job: PostgresSQL connection is closed.')