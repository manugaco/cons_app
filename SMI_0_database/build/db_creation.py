import os
import logging
from datetime import datetime
from datetime import datetime as dt
import psycopg2
import json

from utils import *

## GLOBAL Params:

with open('../config/global.json') as config_file:
    gb_config = json.load(config_file)

headers = gb_config['headers']
urls = gb_config['urls']
ini_users_dict = gb_config['ini_users_dict']

## APP Params:

with open('../config/db_creation.config') as config_file:
    app_config = json.load(config_file)

# Paths
queries_path = app_config['queries_path']
logs_path = app_config['logs_path']
temp_data_path = app_config['temp_data_path']
config_path = app_config['config_path']

# Files:
db_munlist_bkp = app_config['db_munlist_bkp']

# Dates:
db_users_bkp = app_config['db_users_bkp']
db_ini_users_bkp = app_config['db_ini_users_bkp']
db_today = str(dt.today().strftime("%Y-%m-%d"))

# Names:
initial_users_table = app_config['initial_users_table']
users_table = app_config['users_table']
corpus_table = app_config['corpus_table']
munlist_table = app_config['munlist_table']
app_name = app_config['app_name']

## Check and create directories:

if not os.path.isdir(logs_path):
    os.makedirs(logs_path)

if not os.path.isdir(logs_path + app_name):
    os.makedirs(logs_path + app_name)

if not os.path.isdir(temp_data_path):
    os.makedirs(temp_data_path)

## Logger initialization:

api_logger = logging.getLogger(__file__)
file_name = datetime.today().strftime('%y%m%d') + '_' + app_name + '.log'

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
    
    with open(config_path + 'postgres.config') as f:
        db_config = json.load(f)
    
    schema = db_config['db_schema']

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

    api_logger.info('Database jobs: PostgresSQL connection ready.')

    #Create class instance:
    dbcreate = DatabaseCreation(queries_path, conn, schema, 
                                api_logger, urls, headers, 
                                ini_users_dict, initial_users_table, 
                                users_table, corpus_table, munlist_table)

    ## Check schema and tables:
    dbcreate.db_init()

    ## Check backups, tables and fill database:

    api_logger.info('Data jobs: Load auxiliar data from files.')

    ##Load stopwords:
    stops = ['/db_stopwords_spanish_1.txt', '/db_stopwords_spanish_2.txt']
    stopw = dbcreate.get_stops(temp_data_path + 'utils', stops)
    ##Load ecolist:
    files = ['/ecofilter.xlsx']
    ecolist = dbcreate.get_ecofilter(temp_data_path + 'utils', files)

    ## Insert municipalities:
    dbcreate.insert_munlist(temp_data_path)

    # Get municipalities from DB:
    db_munlist = dbcreate.fetchall_SQL(queries_path + 'SMI_munlist_query.sql')
    munlist = pd.DataFrame(db_munlist, columns = ['location'])['location'].tolist()
    db_munlist = [dbcreate.accent_rem(mun.lower().replace(',', '')) for mun in munlist]
    
    ## Insert initial users:
    dbcreate.insert_ini_users(temp_data_path, db_today)

    ## Insert users:
    dbcreate.insert_users(temp_data_path, db_users_bkp, db_munlist)

    ## Insert corpus:
    dbcreate.insert_corpus(temp_data_path, stopw, ecolist)

    ## Insert Tweets:
    dbcreate.insert_tweets(temp_data_path, stopw, ecolist)

except (Exception, psycopg2.DatabaseError) as error:
    logging.exception(error)
    finally_exit = 1
