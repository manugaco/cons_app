
import psycopg2
from io import StringIO


class CommonFunctions:
    '''
    Common functions of multiple smi procesess:
    '''
    def __init__(self,
                queries_path,
                conn,
                schema,
                api_logger
                ):

        #Local parameters:
        self.queries_path = queries_path
        self.conn = conn
        self.cur = conn.cursor()
        self.schema = schema
        self.api_logger = api_logger

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
        # Read the SQL query from .sql file:
        with open(path, 'r') as f: 

            query = f.read().format(schema=self.schema)

        # Initialize SQL cursor:
        cur = self.conn.cursor()

        try:
            
            #Execute query
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