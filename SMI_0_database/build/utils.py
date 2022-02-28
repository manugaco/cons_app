import pandas as pd
import numpy as np
from datetime import datetime as dt
import re
import requests
import psycopg2
from io import StringIO
import os
import json

from bs4 import BeautifulSoup
import spacy
nlp = spacy.load('es_core_news_sm')

class DatabaseCreation:
    '''
    Database creation and initial data insertion:
    '''
    def __init__(self,
                queries_path,
                conn,
                schema,
                api_logger,
                api,
                urls,
                headers,
                ini_users_dict,
                users_table,
                corpus_table,
                munlist_table
                ):

        #Local parameters
        self.queries_path = queries_path
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = schema
        self.api = api
        self.api_logger = api_logger

        #Global parameters
        self.url = urls
        self.header = headers
        self.ini_users_dict = ini_users_dict
        self.users_table = users_table
        self.corpus_table = corpus_table
        self.munlist_table = munlist_table

    ## DATABASE QUERY FUNCTIONS:

    def fetchone_SQL(self, path):
        """
        Function to fetch one observation from a query to database:
        params:
            - path: relative path to the file.
        """
        # Read the SQL query from .sql file:
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
        Function to fetch all observations from the first column in a query to databasee:
        params:
            - path: relative path to the file.
        """
        # Read the SQL query from .sql file:
        with open(path, 'r') as f: 
            query = f.read().format(schema=self.schema)
        cur = self.conn.cursor()

        try:
            cur.execute(query)
            db_fetch = cur.fetchall()

            #Get first column of dataframe (userName)
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
        # Read the SQL query from .sql file:
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
        cur = self.conn.cursor()

        try:
            #Copy cached dataframe into postgres:
            cur.copy_from(buffer, table, sep=",")
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            cur.close()
            self.api_logger.exception(error)

        cur.close()

    ## DATABASE INITIALIZATION FUNCTION:
    
    def db_init(self):
        '''
        Function to check and create the database schema and tables.
        params: selft referenced, no params.
        '''
        try:
            # Check if the schema exists.
            self.api_logger.info('Database job: Check if SMI schema exists.')
            schema_check = self.fetchone_SQL(self.queries_path + 'SMI_coldstart_schema_check.sql')

            # If schema exist, check tables.
            if schema_check:
                self.api_logger.info('Database job: SMI schema exists on DB.')

                ## Check users table.
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

                ## Check corpus table.
                self.api_logger.info('Database job: Check if corpus table exist on DB.')
                db_corpus_check = self.fetchone_SQL(self.queries_path + 'SMI_corpus_table_check.sql')

                # If exists, do nothing.
                if db_corpus_check:
                    self.api_logger.info('Database job: Corpus table exist on DB.')

                # If it does not exist, create corpus table.
                else:
                    self.api_logger.info('Database job: Corpus table does not exist on DB.')
                    self.api_logger.info('Database job: Creating corpus table on DB.')
                    self.query_SQL(self.queries_path + 'SMI_corpus_table_creation.sql')
                    self.api_logger.info('Database job: Corpus table created on DB.')

                ## Check munlist table.
                self.api_logger.info('Database job: Check if municipalities table exist on DB.')
                db_munlist_check = self.fetchone_SQL(self.queries_path + 'SMI_munlist_table_check.sql')

                # If exists, do nothing.
                if db_munlist_check:
                    self.api_logger.info('Database job: Municipalities table exist on DB.')

                # If it does not exist, create munlist table.
                else:
                    self.api_logger.info('Database job: Municipalities table does not exist on DB.')
                    self.api_logger.info('Database job: Creating municipalities table on DB.')
                    self.query_SQL(self.queries_path + 'SMI_munlist_table_creation.sql')
                    self.api_logger.info('Database job: Municipalities table created on DB.')

                ## Check tweets table.
                self.api_logger.info('Database job: Check if tweets table exist on DB.')
                db_tweets_check = self.fetchone_SQL(self.queries_path + 'SMI_tweets_table_check.sql')

                # If exists, do nothing.
                if db_tweets_check:
                    self.api_logger.info('Database job: Tweets table exist on DB.')

                # If it does not exist, create tweets table.
                else:
                    self.api_logger.info('Database job: Tweets table does not exist on DB.')
                    self.api_logger.info('Database job: Creating tweets table on DB.')
                    self.query_SQL(self.queries_path + 'SMI_tweets_table_creation.sql')
                    self.api_logger.info('Database job: Tweets table created on DB.')

            # If schema does not exists, create schema and tables.
            else:

                self.api_logger.info('Database job: SMI schema does not exist.')
                self.api_logger.info('Database job: Cold start - Creating SMI schema and tables on DB.')
                self.query_SQL(self.queries_path + 'SMI_coldstart_database.sql')
                self.api_logger.info('Database job: DB schema and tables created.')

        except Exception as error:
            self.api_logger.exception(error)

    ## INITIAL USERS FUNCTIONS:

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

    def scrap_users(self, temp_data_path, db_today, db_munlist):
        '''
        Function to scrap initial users from url:
        params: self referenced, no params.
        '''
        try:

            # Scrap ini users, create backup and fill database table:
            df = self.get_tw_users_list()
            
            # Treat users:
            self.api_logger.info('Data job: Treat initial users.')
            df_ini_list = df['screenName'].tolist()
            df_users = pd.DataFrame(columns=['smi_str_userid', 
                                 'smi_str_usersname', 
                                 'smi_int_followers', 
                                 'smi_int_friends', 
                                 'smi_bool_protected',
                                 'smi_str_location',
                                 'smi_str_lang'])

        except Exception as error:
            self.api_logger.exception(error)

        for user in df_ini_list:
            try:
                user_obj = self.api.get_user(screen_name = user)
                df_users = pd.concat([df_users, 
                                pd.DataFrame([[user_obj.id, 
                                                user_obj.screen_name, 
                                                user_obj.followers_count,
                                                user_obj.friends_count,
                                                user_obj.protected,
                                                user_obj.location,
                                                user_obj.lang]], columns=['smi_str_userid', 
                                                                        'smi_str_usersname', 
                                                                        'smi_int_followers', 
                                                                        'smi_int_friends', 
                                                                        'smi_bool_protected',
                                                                        'smi_str_location',
                                                                        'smi_str_lang'])], axis=0)

            except Exception as error:
                pass

        try:
            df = df_users.reset_index(drop=True)
            df['smi_str_lastlookup'] = ''

            self.api_logger.info('Data job: Initial users treated.')

            self.api_logger.info('Data job: Filter users by location.')
            # Filter by location:
            df = self.filter_usrs_loc(df, 'smi_str_location', db_munlist)

            # Save initial users to backup:
            self.api_logger.info('Data job: Save initial users to json backup.')
            df.to_json(temp_data_path + 'get_users/db_users_' + db_today + '.json', orient='records', date_format='iso')
            
            # Store initial users on DB:
            self.api_logger.info('Database job: Insert initial users on DB.')
            self.df_to_postgres(df, self.users_table)
            self.api_logger.info('Database job: Initial users inserted on DB.')

        except Exception as error:
            self.api_logger.exception(error)

    ## USERS FUNCTIONS:

    def accent_rem(self, name):
        '''
        Function to remove accents from an alphanumeric string:
        params:
            - name: character string.
        Output: string without accents.
        '''
        try:
            #Define replacements (possible accents or other special char)
            replacements = (
                ("á", "a"),
                ("é", "e"),
                ("í", "i"),
                ("ó", "o"),
                ("ú", "u"),
                ("ñ", 'n'),
                ("à", "a"),
                ("è", "e"),
                ("ì", "i"),
                ("ò", "o"),
                ("ù", "u"),
                ("ä", 'a'),
                ("ë", "e"),
                ("ï", "i"),
                ("ö", "o"),
                ("ü", "u"),
            )
            #Replace with tuple:
            for a, b in replacements:
                name = name.replace(a, b).replace(a.upper(), b.upper())
            return(name)
        except Exception as error:
            self.api_logger.exception(error)

    def filter_usrs_loc(self, df, loc_column, munlist):
        '''
        Function to filter the location field given a municipalities list, to ensure spanish users:
        params:
            - df: input dataframe with users information:
            - munlist: list of municipalities:
        Output: filtered users table.
        '''
        try:
            # Adapt location string
            df[loc_column] = df[loc_column].apply(lambda r: ' '.join([self.accent_rem(word.lower().replace(',', '')) for word in r.split()]))

            # Filter location:
            df = df[df[loc_column].isin(munlist)]
            return(df)

        except Exception as error:
            self.api_logger.exception(error)

    def users_backup(self, path, db_users_bkp, db_munlist):
        '''
        Function to check whether there are backups.
        params:
            - path: relative path to the backup file.
        Output: 
            - df: users dataframe.
            - usr_ls: list of screenName users.
            - check: boolean to check whether there are backup or not.
        '''
        try:
            self.api_logger.info('Data job: Check if backup exists.')
            #Create complete path:
            path = path + 'get_users/db_users_' + db_users_bkp + '.json'
            #Check if file exist:
            if os.path.isfile(path):
                self.api_logger.info('Data job: Users backup exists. Loading file: ' + path)
                #If file exist, open:
                with open(path, 'r') as f:
                    #Normalize json to pandas:
                    df = pd.json_normalize(json.load(f))
                    df.rename(columns={'location':'smi_str_location'}, inplace = True)
                    #Filter users by locaiton:
                    df = self.filter_usrs_loc(df, 'smi_str_location', db_munlist)
                    self.api_logger.info('Data Engineering job: Observations after filter: ' + str(df.shape[0]))
                    #Check if backup has lookup column:
                    if 'smi_str_fflastlookup' not in df:
                        df['smi_str_fflastlookup'] = ''
                    #Select columns:
                    df = df[['id', 'screenName', 'followersCount', 'friendsCount', 'protected', 'smi_str_location', 'lang', 'smi_str_fflastlookup']]
                    df = df.reset_index(drop=True)
                    #Rename columns to database format:
                    df.rename(columns={'id':'smi_str_userid',
                                   'screenName':'smi_str_usersname',
                                   'followersCount':'smi_int_followers',
                                   'friendsCount':'smi_int_friends',
                                   'protected':'smi_bool_protected',
                                   'location':'smi_str_location',
                                   'lang':'smi_str_lang',
                                    }, inplace=True)
                    check = True
                    self.api_logger.info('Data job: Users backup from json file retrieved.')
            else:
                self.api_logger.info('Data job: Users backup does not exists. ')
                df = pd.DataFrame()
                check = False

            return(df, check)

        except Exception as error:

            self.api_logger.exception(error)

    ## TEXT TREATMENT FUNCTIONS:

    def get_stops(self, path, stops):
        '''
        Function to load and treat stopwords.
        params:
            - path: route where the stopwords files would be.
            - stops: list of the files containing stopwords.
        output: stopwords list treated.
        '''
        try:

            with open(path + stops[0]) as f:
                stopw_1 = f.read().splitlines()
            with open(path + stops[1]) as f:
                stopw_2 = f.read().splitlines()

            stopw_1[0] = stopw_1[0].replace('\ufeff', '')
            stopw = stopw_1 + stopw_2
            stopw = [self.accent_rem(word) for word in stopw]
            return(stopw)
        
        except Exception as error:

            self.api_logger.exception(error)

    def get_ecofilter(self, path, files):
        '''
        Function to get the list of words to filter the tweets
        params:
            - path: route where the filtering list files would be.
            - files: list of the files containing filtering words (if there are many).
        '''
        try:

            eco_filter = pd.DataFrame()
            for file in files:
                eco_filter = pd.concat([eco_filter, pd.read_excel(path + file, header=None)], axis=0)
            eco_filter.drop_duplicates(inplace=True)
            eco_filter = eco_filter.iloc[:,0].to_list()
            #eco_filter = [self.lemmatize(word) for word in eco_filter]
            return [self.accent_rem(word) for word in eco_filter]

        except Exception as error:

            self.api_logger.exception(error)

    def tweet_cleaner(self, tweet, stopw, ecol):
        '''
        Function to treat the text of a tweet.
        params:
            - tweet: the document itself.
        output: the tweet cleaned.
        '''
        # Remove urls (http in advance)
        tweet = re.sub(r'http.*',"", tweet)
        tweet = re.sub(r'pic.twitter\S+', '', tweet)
        # Remove mentions and hastags.
        tweet = re.sub(r'#\S+', '', tweet)
        tweet = re.sub(r'@\S+', '', tweet)
        # Remove spanish vowel accents.
        tweet = ' '.join([self.accent_rem(word) for word in tweet.split()])
        # Remove special characters.
        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ", tweet).split())
        # Lower captions.
        tweet = tweet.lower()
        # Remove numbers.
        tweet = ''.join([i for i in tweet if not i.isdigit()])
        # Remove white spaces.
        tweet = re.sub(' +', ' ', tweet)
        # Remove stopwords.
        tweet = ' '.join([word for word in tweet.split() if not word in stopw])
        # Filter words length (<1 and >15).
        tweet = ' '.join([word for word in tweet.split() if len(word) > 1 and len(word) <= 15])
        # Filter ecolist.
        commons = [word for word in ecol if word in tweet]
        if len(commons) < 1:
            tweet = ''
        #if len(tweet) > 1:
        #   #Lemmatize:
        #    tweet = ' '.join([tok.lemma_.lower() for tok in nlp(tweet)])
        return tweet

    def treat_text(self, df, text_col, stopw = [], ecol =[], date_col = 'date', sent_col = 'sentiment'):
        '''
        Function to treat text columns:
        params:
            - df: dataframe to treat.
            - text_col: name of the text columns to treat.
            - ecolist
        Output: Dataframe treated.
        '''
        try:

            # Sanity checks :
            df = df.fillna('')
            
            # Formatting corpus columns:
            if date_col == 'date':
                self.api_logger.info('Text mining job: Format date column.')
                df[date_col] = pd.to_datetime(df[date_col])
            if sent_col == 'sentiment':
                self.api_logger.info('Text mining job: Format sentiment column.')
                df[sent_col] = df[sent_col].replace(',','', regex=True)
                df[sent_col] = df[sent_col].apply(lambda r: r.split('AGREEMENT')[0])
                df[sent_col] = df[sent_col].apply(lambda r: r.split('DI')[0])

            #Column text treatment:
            self.api_logger.info('Text mining job: Treat text column.')
            df[text_col] = df[text_col].fillna(' ')
            df[text_col] = df[text_col].apply(lambda r: self.tweet_cleaner(r, stopw, ecol))
            df = df[df[text_col] != '']
            if text_col == 'text':
                df = df[['username', 'date', 'text']]
            df = df.reset_index(drop=True)
            return(df)
            
        except Exception as error:

            self.api_logger.exception(error)

    def format_tweets_input(self, path, dir, file, stopw, ecolist):
        '''
        Function to insert tweets on database after format them from 
        a backup json file inside a directory recursively.
        params:
            - path: path to backup directory.
            - dir: name of directory.
            - file: name of the file
        '''
        try:
            # Provided the parameters, it loads the json file.
            with open(path + dir + '/' + file, 'r') as f:
                df = pd.DataFrame(json.load(f))
            
            ## INSERT DISTINCT DATES INTO DB:

            # Once the file is loaded, the tweets are treated.
            df = self.treat_text(df, 'text', stopw, ecolist, date_col = None, sent_col = None)

            if df.shape[0] > 0:

                df = df[df["text"] != '']
                # Once the text is treated, it is persisted on the database.
                df = df[['username', 'date', 'text']]
                self.df_to_postgres(df, 'smi_tweets')

        except Exception as error:
            print(error)

    ## INSERTION FUNCTIONS:

    def insert_munlist(self, temp_data_path):
        '''
        Function to insert municipalities into DB.
        params: self referenced, no params.
        '''
        try:

            self.api_logger.info('Database job: Get number of observation of municipalities table.')
            n_obs = self.fetchone_SQL(self.queries_path + 'SMI_munlist_count.sql')
            self.api_logger.info('Database job: Number of observation on municipalities table: ' + str(n_obs))

            if n_obs == 0:

                path_munlist = temp_data_path + 'municipalities/munlist.json'

                with open(path_munlist, 'r') as f:

                    self.api_logger.info('Data job: Retrieve municipalities file.')
                    df = pd.DataFrame(json.load(f), columns=['location'])
                    df['location'] = df['location'].replace(',','', regex=True)
                    self.api_logger.info('Data job: Municipalities number of observations: ' + str(df.shape[0]) + ' observations.')
                    df.drop_duplicates(inplace = True)
                    self.api_logger.info('Data job: Drop duplicates of unicipalities: ' + str(df.shape[0]) + ' observations.')
                    self.api_logger.info('Database job: Insert municipalities into DB.')
                    self.df_to_postgres(df, self.munlist_table)
                    self.api_logger.info('Database job: Municipalities inserted into DB.')
            
            else:

                self.api_logger.info('Database job: Municipalities table already filled.')

        except Exception as error:

            self.api_logger.info('Data job: Municipalities file not found, please provide a municipalities json file.')
            self.api_logger.exception(error)

    def insert_users(self, queries_path, temp_data_path, db_users_bkp, db_munlist, db_today):
        '''
        Function to insert users backup into DB.
        params: self referenced, no params.
        '''
        try:

            ## Check if users table exists in DB (query)
            self.api_logger.info('Database job: Check if users table exists.')
            db_usr_check = self.fetchone_SQL(queries_path + 'SMI_usrs_table_check.sql')

            if db_usr_check:
                self.api_logger.info('Database job: Users table exists.')
                ## If users table exists:
                ## Check number of observations (query)
                n_obs = self.fetchone_SQL(self.queries_path + 'SMI_usrs_count.sql')
                self.api_logger.info('Database job: Number of observations of users table: ' + str(n_obs))

                if n_obs == 0:
                    ## If users table n_obs == 0:
                    ## Check users backup.
                    self.api_logger.info('Database job: Users table in DB is empty.')
                    self.api_logger.info('Data job: Check users backup.')
                    df_usr, df_usr_check = self.users_backup(temp_data_path, db_users_bkp, db_munlist)
                
                    if df_usr_check:
                        # Users table insertion:
                        self.api_logger.info('Database job: Users backup exists, insert into DB.')
                        self.df_to_postgres(df_usr, self.users_table)
                        self.api_logger.info('Database job: Users table inserted on DB.')
                        
                    else:
                        self.api_logger.info('Data job: Users backup does not exists.')
                        ## If users backup does not exists:
                        ## Scrap users from url and insert into DB.
                        self.api_logger.info('Scrapping job: Retrieve users from url.')
                        self.scrap_users(temp_data_path, db_today, db_munlist)
                        self.api_logger.info('Scrapping job:Users retrieved from url.')

                elif n_obs > 0:
                    ## If users table n_obs > 0
                    ## Do nothing
                    self.api_logger.info('Database job: Users table already filled.')

                # Drop duplicates from users table:
                self.api_logger.info('Data Engineering job: Droping duplicated users from users table on DB.')
                self.query_SQL(self.queries_path + 'SMI_usrs_remove_dups.sql')
                n_obs_dups = self.fetchone_SQL(self.queries_path + 'SMI_usrs_count.sql')
                self.api_logger.info('Database job: Number of observations in the users table on DB after droping duplicates: ' + str(n_obs_dups))
            
            else:

                self.api_logger.info('Database job: Users table does not exist.')
                self.api_logger.info('Database job: Create users table.')
                ## If users table does not exists:
                ## Create users table:
                self.query_SQL(queries_path + 'SMI_usrs_table_creation.sql')
                ## Check users backup.
                self.api_logger.info('Data job: Check users backup.')
                df, check = self.users_backup(temp_data_path, db_users_bkp, db_munlist)
                if check:
                    self.api_logger.info('Data job: Users backup exists, insert into db.')
                    ## If users backup exists:
                    ## Insert users backup into DB
                    self.df_to_postgres(df, self.users_table)
                    self.api_logger.info('Data job: Users backup inserted into db.')
                else:
                    self.api_logger.info('Data job: Users backup does not exists, insert into db.')
                    ## If users backup does not exists:
                    ## Scrap users from url and insert into DB.
                    self.api_logger.info('Scrapping job: Retrieve users from url.')
                    self.scrap_users(temp_data_path, db_today, db_munlist)
                    self.api_logger.info('Scrapping job:Users retrieved from url.')

        except Exception as error:
            self.api_logger.exception(error)

    def insert_corpus(self, temp_data_path, stopw, ecolist):
        '''
        Function to insert corpus into DB.
        params:
            - temp_data_path: path to data folder.
            - stopw: list with stop words to remove.
            - ecolist: list of terms which a tweet must contain at least one.
        output: No output, the results are persisted directly into DB.
        '''
        try:

            self.api_logger.info('Database job: Get number of observation of corpus table.')
            n_obs = self.fetchone_SQL(self.queries_path + 'SMI_corpus_count.sql')
            self.api_logger.info('Database job: Number of observation on corpus table: ' + str(n_obs))

            if n_obs == 0:

                path_corpus = temp_data_path + 'train_model/TASScorpus.json'

                with open(path_corpus, 'r') as f:

                    self.api_logger.info('Data job: Retrieve corpus file.')
                    df = pd.json_normalize(json.load(f))
                    self.api_logger.info('Data job: Treat corpus text column.')
                    df = self.treat_text(df, 'content', stopw, ecolist)
                    df = df[['user', 'date' ,'content', 'sentiment']]
                    self.api_logger.info('Data job: Corpus text column treated.')
                    self.api_logger.info('Data job: Corpus number of observations: ' + str(df.shape[0]) + ' observations.')
                    df.drop_duplicates(inplace = True)
                    self.api_logger.info('Data job: Drop duplicates of corpus: ' + str(df.shape[0]) + ' observations.')
                    self.api_logger.info('Database job: Insert corpus into DB.')
                    self.df_to_postgres(df, self.corpus_table)
                    self.api_logger.info('Database job: Corpus inserted into DB.')
                    
            else:

                self.api_logger.info('Database job: Corpus table already filled.')

        except Exception as error:

            self.api_logger.info('Data job: Corpus file not found, please provide a corpus json file.')
            self.api_logger.exception(error)

    def insert_tweets(self, temp_data_path, stopw, ecolist):
        '''
        Function to insert tweets into DB.
        params: self referenced, no params.
        '''
        try:

            self.api_logger.info('Database job: Getting number of observation of tweets table.')
            n_obs = self.fetchone_SQL(self.queries_path + 'SMI_tweets_count.sql')
            self.api_logger.info('Database job: Number of observation on tweets table: ' + str(n_obs))

            if n_obs == 0:

                path_tweets = temp_data_path + 'get_tweets/'
                dirs = os.listdir(path_tweets)
                for dire in dirs:
                    files = os.listdir(path_tweets + dire + '/')
                    for file in files:
                        self.api_logger.info('Database job: Inserting file on DB: ' + file)
                        self.format_tweets_input(path_tweets, dire, file, stopw, ecolist)
                        self.api_logger.info('Database job: File inserted on DB: ' + file)
                        n_obs = self.fetchone_SQL(self.queries_path + 'SMI_tweets_count.sql')
                self.api_logger.info('Database job: Number of observations in the tweets table on DB before droping duplicates: ' + str(n_obs))
                self.query_SQL(self.queries_path + 'SMI_tweets_remove_dups.sql')
                n_obs = self.fetchone_SQL(self.queries_path + 'SMI_tweets_count.sql')
                self.api_logger.info('Database job: Number of observations in the tweets table on DB after droping duplicates: ' + str(n_obs))
                    
            else:
                self.api_logger.info('Database job: Tweets table already filled.')

        except Exception as error:

            self.api_logger.info('Data job: Tweets backup files not found, please provide tweets json file.')
            self.api_logger.exception(error)