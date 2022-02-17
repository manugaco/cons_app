import pandas as pd
import numpy as np
import re
import requests
import psycopg2
from io import StringIO
import os
import json

from bs4 import BeautifulSoup

class DatabaseCreation:
    '''
    Database creation and initial data insertion:
    '''
    def __init__(self,
                queries_path,
                conn,
                schema,
                api_logger,
                urls,
                headers,
                ini_users_dict,
                initial_users_table,
                users_table,
                corpus_table
                ):

        #Local parameters
        self.queries_path = queries_path
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = schema
        self.api_logger = api_logger
        #Global parameters
        self.url = urls
        self.header = headers
        self.ini_users_dict = ini_users_dict
        self.initial_users_table = initial_users_table
        self.users_table = users_table
        self.corpus_table = corpus_table

    def get_info(self, url, header):
        '''
        Function to get the initial twitter users file.
        Params: 
            - url: list of url pages with the most followed spanish twitter accounts.
            - header: specifies robots.txt user agent.
        Output: Uncleaned dataframe with parsed tables.
        '''
        try:

            #Get text from url:
            page = requests.get(url, headers = header)
            soup = BeautifulSoup(page.text, "lxml")
            results = soup.find(id="listado")
            
            #Get table from text:
            df = pd.DataFrame([[tr for tr in tab] for tab in results.table])
            df = df.loc[:,1:]

            #Get column names:
            colnames = [str(name) for name in df.iloc[0]]
            colnames = [re.search('<b>(.*)</b>', name).group(1).replace('<br/>', ' ') for name in colnames if name != 'None']
            df = df.loc[1:,2:]
            df.columns = colnames

            #Dropping columns:
            df = df[~df['Twittero'].isnull()].reset_index(drop=True)
            return(df)

        except Exception as error:
            self.api_logger.exception(error)

    def get_user(self, input):
        '''
        Function to extract user name from the list.
        Params:
            - input: twitter message.
        Output: Twitter user.
        '''
        try:
            #Locating the username:
            s = str(input)
            start = s.find(">@") + len(">@")
            end = s.find("<br/")
            substring = s[start:end]

            #Returning the username from:
            return(substring)

        except Exception as error:
            self.api_logger.exception(error)

    def get_n(self, input):
        '''
        Function to extract numeric values from the table:
        Params:
            - input: Different values from twitter users accounts.
        Output: Value
        '''
        try:
            #Locating the value;
            s = str(input)
            start = s.find(">") + len(">")
            end = s.find("</td")
            substring = s[start:end]

            #Returning the value:
            return(substring.replace(',', ''))

        except Exception as error:
            self.api_logger.exception(error)

    def get_initial_users_table(self, url, header):
        '''
        Function to create the initial users dataframe.
        Params:
            - url: list of url pages with the most followed spanish twitter accounts.
            - header: specifies robots.txt user agent.
        Output: Dataframe with cleaned data.
        '''
        try:
            #Scrap info from wp:
            info = self.get_info(url, header)

            #Columns to clean:
            cols = ['Twittero', 'Seguido por', 'Sigue a', 'Tweets', 'Twitea desde', 'Ultimo Tweet', 'Categoria']

            #Clean columns:
            for col in cols:
                if col == 'Twittero':
                    users = [self.get_user(info[col][i]) for i in range(info.shape[0])]
                    info[col] = users
                else:
                    info[col] = [self.get_n(info[col][i]) for i in range(info.shape[0])]
            return(info)

        except Exception as error:
            self.api_logger.exception(error)

    def get_tw_users_list(self):
        '''
        Function to get all users from different urls
        Params:
            - url: list of url pages with the most followed spanish twitter accounts.
            - header: specifies robots.txt user agent.
        Output: Users list and users table
        '''
        #Get all users from tables of different sections:
        try:
            users = []
            df_out = pd.DataFrame()
            for i in range(len(self.url)):
                
                #Create users dataframe and users list:
                df = self.get_initial_users_table(self.url[i][0], self.header)
                df_out = pd.concat([df_out, df], axis=0).reset_index(drop=True)
                users.extend(df['Twittero'].to_list())

            #Drop duplicates:
            if len(users) != len(set(users)):
                users = list(set(users))
                df_out = df_out.drop_duplicates().reset_index(drop=True)

            #Formating columns:
            df_out.columns = [key for key in list(self.ini_users_dict.keys())]
            df_out = df_out.astype(self.ini_users_dict)
            df_out['lastTweet'] = np.where(df_out['lastTweet']=='n/d', df_out['tweetsSince'], df_out['lastTweet'])
            df_out['tweetsSince']=pd.to_datetime(df_out['tweetsSince'])
            df_out['lastTweet']=pd.to_datetime(df_out['lastTweet'])
            df_out = df_out.fillna(0)
            return(df_out)

        except Exception as error:
            self.api_logger.exception(error)

    def filter_usrs_loc(self, df, munlist):
        '''
        Function to filter the location field given a municipalities list, to ensure spanish users:
        params:
            - df: input dataframe with users information:
            - munlist: list of municipalities:
        Output: filtered users table.
        '''
        try:
            # Adapt location string
            df['location'] = df['location'].apply(lambda r: r.replace(',', ''))
            # Convert location field to lower case:
            df['location'] = df['location'].apply(lambda r: r.lower())
            # Filter location:
            df = df[df['location'].isin(munlist)]
            return(df)

        except Exception as error:
            self.api_logger.exception(error)

    def backup_check(self, path, db_munlist, kind):
        '''
        Function to check whether there are backups.
        params:
            - path: relative path to the backup file.
            - kind: initial users or users.
        Output: 
            - df: users dataframe.
            - usr_ls: list of screenName users.
            - check: boolean to check whether there are backup or not.
        '''
        try:
            self.api_logger.info('Data job: Check if ' + kind + ' backup exists.')

            if os.path.isfile(path):

                self.api_logger.info('Data job: ' + kind + ' backup exists. Loading file: ' + path)

                with open(path, 'r') as f:

                    df = pd.json_normalize(json.load(f))

                    if kind == 'users':
                        
                        self.api_logger.info('Data Engineering job: Filtering location from backup users.')
                        self.api_logger.info('Data Engineering job: Observations before filter: ' + str(df.shape[0]))
                        df = self.filter_usrs_loc(df, db_munlist)
                        df['ff_lookup'] = False
                        self.api_logger.info('Data Engineering job: Observations after filter: ' + str(df.shape[0]))

                    usr_ls = df['screenName'].to_list()
                    check = True
                    self.api_logger.info('Data job: ' + kind + ' backup from json file retrieved.')
            else:

                self.api_logger.info('Data job: ' + kind + ' backup does not exists. ')
                df = pd.DataFrame()
                usr_ls = []
                check = False
            return(df, usr_ls, check)

        except Exception as error:
            self.api_logger.exception(error)

    def fetchone_SQL(self, path):
        """
        Function to fetch one observation from a query to database:
        params:
            - path: relative path to the file.
        """
        with open(path, 'r') as f:
            query = f.read().format(schema = self.schema)
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            return(cur.fetchone()[0])

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            cur.close()
            self.api_logger.exception(error) 
    
    def fetchall_SQL(self, path):
        """
        Function to fetch all observations from a query to databasee:
        params:
            - path: relative path to the file.
        """
        with open(path, 'r') as f: 
            query = f.read().format(schema=self.schema)
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            db_fetch = cur.fetchall()
            db_fetch = [db_fetch[i][0] for i in range(len(db_fetch))]
            return(db_fetch)

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)
        cur.close()

    def query_SQL(self, path):
        """
        Function to make a query to database:
        params:
            - path: relative path to the file.
        """
        with open(path, 'r') as f:
            query = f.read().format(schema=self.schema)
        cur = self.conn.cursor()
        try:
            cur.execute(query)

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)
        cur.close()

    def df_to_postgres(self, df, table):
        """
        Function to save dataframe into postgres with copy_from:
        params:
            - conn: database connection.
            - df: pandas dataframe.
            - table: database table.
        """
        #Buffering the dataframe into memory:
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)

        #Copy cached dataframe into postgres:
        cur = self.conn.cursor()
        try:
            cur.copy_from(buffer, table, sep=",")
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            cur.close()
            self.api_logger.exception(error)
        cur.close()

    def db_cs(self):
        '''
        Function to check and create the database schema and tables.
        params: selft referenced, no params.
        '''
        try:
            # Check if the schema exists.
            self.api_logger.info('Database job: Check if SMI schema exists.')
            schema_check = self.fetchone_SQL(self.queries_path + 'SMI_schema_check.sql')

            # If schema exist, check tables.
            if schema_check:

                # Check initial users table.
                self.api_logger.info('Database job: SMI schema exists on DB.')
                self.api_logger.info('Database job: Check if initial users table exist on DB.')
                db_ini_check = self.fetchone_SQL(self.queries_path + 'SMI_ini_users_check.sql')

                # If exists, do nothing.
                if db_ini_check:
                    self.api_logger.info('Database job: Initial users table exist on DB.')

                # If it does not exist, create initial users table.
                else:
                    self.api_logger.info('Database job: Initial users table does not exist on DB.')
                    self.api_logger.info('Database job: Creating initial users table on DB.')
                    self.query_SQL(self.queries_path + 'SMI_ini_users_table_creation.sql')
                    self.api_logger.info('Database job: Initial users table created on DB.')

                # Check users table.
                self.api_logger.info('Database job: Check if users table exist on DB.')
                db_usr_check = self.fetchone_SQL(self.queries_path + 'SMI_usrs_table_check.sql')

                #If exists, do nothing.
                if db_usr_check:
                    self.api_logger.info('Database job: Users table exist on DB.')

                #If it does not exist, create users table.
                else:
                    self.api_logger.info('Database job: Users table does not exist on DB.')
                    self.api_logger.info('Database job: Creating users table on DB.')
                    self.query_SQL(self.queries_path + 'SMI_usrs_table_creation.sql')
                    self.api_logger.info('Database job: Users table created on DB.')

            # If schema does not exists, create schema and tables.
            else:
                self.api_logger.info('Database job: SMI schema does not exist.')
                self.api_logger.info('Database job: Cold start - Creating SMI schema and tables on DB.')
                self.query_SQL(self.queries_path + 'SMI_coldstart_database.sql')
                self.api_logger.info('Database job: DB schema and tables created.')

        except Exception as error:
            self.api_logger.exception(error)

    def scrap_users(self, temp_data_path, db_today):
        '''
        Function to scrap initial users from url:
        params: self referenced, no params.
        '''
        try:
            # Scrap ini users, create backup and fill database table:
            self.api_logger.info('Scraping job: Retrieve initial users from url.')
            df = self.get_tw_users_list()
            self.api_logger.info('Scraping job: Initial users from url retrieved.')
            self.api_logger.info('Scraping job: Save initial users to json backup.')
            df.to_json(temp_data_path + 'get_users/db_ini_users_' + db_today + '.json', orient='records', date_format='iso')
            self.df_to_postgres(df, self.initial_users_table)
            self.api_logger.info('Database job: Initial users table created on DB.')

        except Exception as error:
            self.api_logger.exception(error)

    def insert_ini_users(self, temp_data_path, db_today, db_ini_users_bkp, db_munlist):
        '''
        Function to insert initial users into DB.
        params: self referenced, no params.
        '''
        try:

            db_ini_ls = self.fetchall_SQL(self.queries_path + 'SMI_ini_database_screenName.sql')
            path_ini = temp_data_path + 'get_users/db_ini_users_' + db_ini_users_bkp + '.json'
            df_ini_users, df_ini_ls, df_ini_user_check = self.backup_check(path_ini, db_munlist, kind = 'initial users')

            if df_ini_user_check:
                
                if (len(db_ini_ls) == 0):
                    self.api_logger.info('Database job: Initial users table is empty.')
                    self.api_logger.info('Database job: Insert initial users backup into DB.')
                    self.df_to_postgres(df_ini_users, self.initial_users_table)
                else:
                    self.api_logger.info('Database job: Initial users table is not empty.')
                    self.api_logger.info('Data job: Compare initial users on DB and backup.')

                    df_ini_ls.sort()
                    db_ini_ls.sort()
                    
                    if df_ini_ls == db_ini_ls:
                        self.api_logger.info('Data job: initial users match.')
                    else:
                        self.api_logger.info('Database job: initial users do not match, drop and create initial users table.')
                        #Create initial users table:
                        self.api_logger.info('Database job: Creating initial users table on DB.')
                        self.query_SQL(self.queries_path + 'SMI_ini_users_table_creation.sql')
                        
                        #Initial users table insertion:
                        self.api_logger.info('Database job: Insert initial users back into DB.')
                        self.df_to_postgres(df_ini_users, self.initial_users_table)
                        self.api_logger.info('Database job: Initial users table inserted on DB.')
            else:
                self.scrap_users(temp_data_path, db_today)

        except Exception as error:
            self.api_logger.exception(error)

    def insert_users(self, temp_data_path, db_users_bkp, db_munlist):
        '''
        Function to insert users backup into DB.
        params: self referenced, no params.
        '''
        try:
            db_usr_ls = self.fetchall_SQL(self.queries_path + 'SMI_usrs_database_screenName.sql')
            path_usr = temp_data_path + 'get_users/db_users_' + db_users_bkp + '.json'
            df_usr, df_usr_ls, df_usr_check = self.backup_check(path_usr, db_munlist, kind = 'users')

            if df_usr_check:
                
                if (len(db_usr_ls) == 0):
                    self.api_logger.info('Database job: Users table is empty.')
                    self.api_logger.info('Database job: Insert users backup into DB.')
                    self.df_to_postgres(df_usr, self.users_table)
                else:
                    self.api_logger.info('Database job: Users table is not empty.')
                    self.api_logger.info('Data job: Compare users on db and backup.')

                    df_usr_ls.sort()
                    db_usr_ls.sort()
                    
                    if df_usr_ls == db_usr_ls:
                        self.api_logger.info('Data job: Users match.')
                    else:
                        self.api_logger.info('Database job: Users do not match.')
                        if len(df_usr_ls) > len(db_usr_ls):
                            #Create initial users table:
                            self.api_logger.info('Database job: Creating users table on DB.')
                            self.query_SQL(self.queries_path + 'SMI_usrs_table_creation.sql')
                        
                            #Initial users table insertion:
                            self.api_logger.info('Database job: Users back into DB.')
                            self.df_to_postgres(df_usr, self.users_table)
                            self.api_logger.info('Database job: Users table inserted on DB.')

                self.api_logger.info('Data Engineering job: Droping duplicated users from users table on DB.')
                self.query_SQL(self.queries_path + 'SMI_usrs_remove_dups.sql')
                n_obs = self.fetchone_SQL(self.queries_path + 'SMI_usrs_count_users.sql')
                self.api_logger.info('Database job: Number of observations on the users table after droping duplicates: ' + str(n_obs))

        except Exception as error:
                self.api_logger.exception(error)

    def insert_corpus(self, temp_data_path, db_munlist):
        '''
        Function to insert users backup into DB.
        params: self referenced, no params.
        '''
        try:
            path_corpus = temp_data_path + 'train_model/TASScorpus.json'
            with open(path_corpus, 'r') as f:
                df = pd.json_normalize(json.load(f))
                self.api_logger.info('Database job: Insert corpus into DB.')
                self.df_to_postgres(df, self.corpus_table)
                self.api_logger.info('Database job: Corpus inserted into DB.')

        except Exception as error:
                self.api_logger.exception(error)