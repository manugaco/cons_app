from io import StringIO
import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
import psycopg2
from psycopg2 import sql

import spacy
nlp = spacy.load('es_core_news_sm')

import warnings
warnings.filterwarnings('ignore')


class TweetsPipeline:
    '''
    Tweets retrieval and database insertion:
    '''
    def __init__(self,
                queries_path,
                conn,
                schema,
                api_logger
                ):

        #Local parameters
        self.queries_path = queries_path
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = schema
        self.api_logger = api_logger

    ## DATABASE QUERY FUNCTIONS:

    def fetchall_SQL(self, path):
        """
        Function to fetch all observations from a query to databasee:
        params:
            - path: relative path to the file.
        """

        with open(path, 'r') as f: 
            query = f.read().format(schema=self.schema)

        try:
            #Execute query
            cur = self.conn.cursor()
            cur.execute(query)
            db_fetch = cur.fetchall()
            cur.close()
            return(db_fetch)

        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)

    def insert_datetweets_into_db(self, path, user_row):
        '''
        Function to update tweet dates on DB.
        params:
            - user_row: row to insert into the db.
        '''

        try:
            cur = self.conn.cursor()
            with open(path) as f:
                cur.execute(
                    sql.SQL(f.read()).format(schema=sql.Identifier(self.schema)),
                    user_row
                )

                self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            self.conn.rollback()
            self.api_logger.exception(error)

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

    def get_tweets(self, user, date_ini, date_end, stopw, ecolist):
        '''
        Function to get tweets from a user given a period range.
        params:
            - user: twitter user name.
            - date_ini: first day of time window to retrieve tweets.
            - date_end: last date of time window to retrieve tweets.
        '''
        # Tweets list:
        twts_ls = []

        # Twitter scrapper:
        for i, tweet in enumerate(sntwitter.TwitterSearchScraper('from:' + user + ' since:' + date_ini + ' until:' + date_end).get_items()):
            twts_ls.append([tweet.user.username, tweet.date, tweet.content])
            
        # Tweets dataframe: 
        df = pd.DataFrame(twts_ls, columns=['username', 'date', 'text'])
        df = self.treat_text(df, 'text', stopw, ecolist, date_col = 'date', sent_col = None)
        return df