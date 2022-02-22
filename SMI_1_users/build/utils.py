import pandas as pd
import numpy as np
from datetime import datetime as dt
from tweepy import Cursor
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import register_adapter, AsIs

register_adapter(np.int64, AsIs)
register_adapter(np.bool_, AsIs)

import warnings
warnings.filterwarnings("ignore")

class UsersPipeline:
    '''
    Generate new users in the users table of the database:
    '''
    def __init__(self,
                queries_path,
                conn,
                schema,
                api,
                temp_data_path,
                api_logger
                ):
        #Local parameters
        self.queries_path = queries_path
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = schema
        self.api = api
        self.temp_data_path = temp_data_path
        self.api_logger = api_logger

    ## DATABASE QUERY FUNCTIONS:

    def fetchall_SQL(self, path):
        """
        Function to fetch all observations from a query to databasee:
        params:
            - path: relative path to the file.
        """
        # Read the SQL query from .sql file:
        with open(path, 'r') as f: 
            query = f.read().format(schema=self.schema)

        # Initialize SQL cursor:
        cur = self.conn.cursor()

        try:

            #Execute query
            cur.execute(query)
            db_fetch = cur.fetchall()
            
            return(db_fetch)

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)
        cur.close()

    ## TEXT TREATMENT FUNCTIONS:

    def text_clean_loc(self, name):
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
            )
            #Replace with tuple:
            for a, b in replacements:
                name = name.replace(a, b).replace(a.upper(), b.upper())
            return(name)
        except Exception as error:
            self.api_logger.exception(error)

    def clean_loc(self, loc, munlist):
        '''
        Function to flag if the location, after being cleaned, still exist
        params:
            - loc: location.
            - munlist: list of municipalities locations.
        Output: flag which indicates whether the location exists.
        '''
        try:
            
            #Adapt text format to location column:
            loc = self.text_clean_loc(loc.lower().replace(',', ''))

            #Return dicotomic flag if user location is on munlist:
            if loc in munlist:
                return 1
            else:
                return 0
        except Exception as error:
            self.api_logger.exception(error)

    def treat_new_users(self, df, db_munlist):
        '''
        Function to treat the location column and filter non spanish users:
        params:
            - df: Dataframe with the new users.
            - db_munist: list of spanish municipalities to filter the new users.
        Output: Dataframe with only spanish users.
        '''
        try:
            # Split the location column into a list of strings to check if each word is in the list:
            df_t = df.copy()
            df_t['location'] = df_t['location'].apply(lambda r: r.split())

            # Clean the text of the location column:
            locls = df_t['location'].to_list()
            locls_clean = [[self.clean_loc(loc, db_munlist) for loc in userloc] for userloc in locls]
            
            # Filter whether the users are in the municipalities or not:
            df_t = pd.concat([df_t, pd.DataFrame([sum(userloc) for userloc in locls_clean]).rename(columns={0:'loc_filter'})], axis=1).reset_index(drop=True)
            df_t = df_t[df_t['loc_filter']>0]
            df_t.drop(columns='loc_filter', inplace=True)
            
            # Inner join to keep only the spanish users from the original dataframe:
            output = df.merge(df_t['id'], on='id', how='inner')
            output = output.astype({"id": str})
            output['ff_lookup'] = False
            
            return(output)
        except Exception as error:
            self.api_logger.exception(error)

    ## FRIENDS AND FOLLOWERS FUNCTIONS:

    def ff_transform(self, list):
        '''
        Function to adapt the API friends and followers information format:
        params:
            - list: json list given by the twitter API.
        Output: Friends and followers API information in a dataframe with specific columns.
        '''
        try:
            if len(list) > 0:

                #Extract each element from the json file:
                data = [x._json for x in list]

                #Convert to dataframe:
                df = pd.DataFrame(data)

                #Reorder and change column names to match the database table:
                df = df[['id', 'screen_name', 'followers_count', 'friends_count', 'protected', 'location', 'lang']]
                df.rename(columns={'screen_name':'screenName', 'followers_count':'followersCount', 'friends_count':'friendsCount'}, inplace=True)

            else:
                #Return empty dataframe:
                df = pd.DataFrame(columns=['id', 'screenName', 'followersCount', 'friendCount', 'protected', 'location', 'lang'])

            return(df)

        except Exception as error:
            self.api_logger.exception(error)

    def get_ff(self, user, api):
        '''
        Function to get friends and followers from a given users.
        params:
            - user: twitter user screen name.
        Output: dataframe with all the friends and followers of a given twitter user.
        '''
        try:
            
            #Retrieve followers from a given user:
            self.api_logger.info('Twitter API job: Retrieving followers')
            followers = []
            for fid in Cursor(api.get_followers, screen_name=user).items():
                followers.append(fid)

            if len(followers) > 0:
                #Transform format:
                df_followers = self.ff_transform(followers)
            else:
                df_followers = pd.DataFrame()
            self.api_logger.info('Twitter API job: Number of followers retrieved: ' + str(df_followers.shape[0]))
            
            #Retrieve friends from a give user:
            self.api_logger.info('Twitter API job: Retrieving friends')
            friends = []
            for fid in Cursor(api.get_friends, screen_name=user).items():
                friends.append(fid)
                
            if len(friends) > 0:
                #Transform format:
                df_friends = self.ff_transform(friends)
            else:
                df_friends = pd.DataFrame()
            self.api_logger.info('Twitter API job: Number of friends retrieved from user: ' + str(df_friends.shape[0]))

            #Output dataframe:
            output = pd.concat([df_followers, df_friends], axis=0)
            output.drop_duplicates(inplace=True)

            #Adapt id format:
            output = output.astype({"id": object})
            if output.shape[0] > 0:
                return(output.reset_index(drop=True))
            else:
                return(pd.DataFrame())

        except Exception:
            self.api_logger.info('Raised exception, getting info from next user')
            pass

    def update_user_lookup(self, user):
        '''
        Function to update the lookup status of a given user via query.
        params:
            - user: screenName of a user.
        '''
        try:
            self.api_logger.info('Database job: Updating lookup of user')
            with open(self.queries_path + 'SMI_update_lookup_users.sql') as f:
                self.cur.execute(
                    sql.SQL(f.read()).format(schema=sql.Identifier(self.schema)),
                    (user, )
                )
                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)

    def insert_new_users_into_db(self, user_row):
        '''
        Function to check if a new user exists and insert into db in other case.
        params:
            - user_row: row to insert into the db.
        '''

        try:
            with open(self.queries_path + 'SMI_insert_new_user.sql') as f:
                self.cur.execute(
                    sql.SQL(f.read()).format(schema=sql.Identifier(self.schema)),
                    user_row
                )

                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)
    
    def users_backup(self, df):
        '''
        Function to backup users.
        params:
            - df: dataframe of users.
        '''
        try:

            (df.to_json(self.temp_data_path + 
            'get_users/db_users_' + 
            str(dt.today().strftime("%Y-%m-%d")) + 
            '.json', 
            orient='records', 
            date_format='iso')
            )
            
        except Exception as error:
            self.api_logger.exception(error)

    def get_and_insert_new_users(self, user, munlist, api):
        '''
        Function to create new users table from a list of existing users.
        params:
            - usr_list: twitter users list.
        Output: dataframe with new potential users.
        '''
        try:
            
            #Get friends and followers from a given user:
            df_new_users = self.get_ff(user, api)

            #Insert new users into database and drop duplicates:
            if df_new_users.shape[0] > 0:

                df_new_users = self.treat_new_users(df_new_users, munlist)

                self.api_logger.info('Database job: Number of new users to be inserted into DB: ' + str(df_new_users.shape[0]))
                self.api_logger.info('Database job: Insert new users into DB')

                for i in range(df_new_users.shape[0]):
                    self.insert_new_users_into_db(tuple(df_new_users.iloc[i, :]))

                self.api_logger.info('Database job: New users inserted into DB')
                self.api_logger.info('Data job: Saving new users backup' )

                df = pd.DataFrame(self.fetchall_SQL(self.queries_path + 'SMI_query_new_users.sql'))
                df.columns = ['id', 'screenName', 'followersCount', 'friendsCount', 'protected', 'location', 'lang', 'ff_lookup']
                self.users_backup(df)
                
                self.api_logger.info('Data job: New users backup saved')

                #Update ff_lookup column from a given user in database:
                self.update_user_lookup(user)

            else:
                self.api_logger.info('Database job: There are not new users to be inserted into DB')

        except Exception as error:
            self.api_logger.exception(error)