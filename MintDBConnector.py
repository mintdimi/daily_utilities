import numpy as np
import pandas as pd

import mysql.connector
from mysql.connector import Error

import sqlalchemy
from sqlalchemy.engine.url import URL

mintdb_config = dict(
    drivername = 'mysql',
    username = 'dimiter',
    password = 'mint1234',
    host = '192.168.31.12',
    port = '3306',
    database = 'MintDB',
)

# ------------------------------------------------------------------------------

class MintDBConnector_MySQL:
    host = mintdb_config['host']
    user = mintdb_config['username']
    password = mintdb_config['password']
    database = mintdb_config['database']
    
    def __init__(self):
        self.mydb = self.create_db_connection()
    
    def create_db_connection(self, verbose=False):
        connection = None
        try:
            connection = mysql.connector.connect(
                host = self.host,
                user = self.user,
                passwd = self.password,
                database = self.database,
            )
            if verbose:
                print('MySQL database connection successful')
        except Error as e:
            print(f'Error: {e}')
        return connection
    
    def _send_query(self, query):
        cursor = self.mydb.cursor()
        try:
            cursor.execute(query)
            return cursor
        except Error as err:
            print(f'Error: {err}')
        
    def read_query(self, query):
        cursor = self._send_query(query)
        return cursor.fetchall()
            
    def read_query_df(self, query):
        cursor = self._send_query(query)
        return pd.DataFrame(cursor.fetchall(), 
            columns=[x[0] for x in cursor.description])


class MintDBConnector_SQLAlchemy:
    """
    q = "SELECT * FROM MintDB.OTCPosition WHERE PBDealRef = '219453874'"
    b = MintDBConnector_SQLAlchemy()
    
    %timeit -n 10 -r 10 b.read_query(q)
    191 ms ± 4.14 ms per loop (mean ± std. dev. of 10 runs, 10 loops each)
    
    ! this is quite slow compared to the MySQL version
    ! for smaller requests, can get up to a factor of 100x, larger 1.4x
    """
    config = mintdb_config
    
    def __init__(self):
        self._url = self._create_url()
        self.engine = sqlalchemy.create_engine(self._url, echo=False)
        
    def _create_url(self):
        return URL.create(**self.config)
    
    def read_query(self, query):
        res = self.engine.execute(query)
        return res.fetchall()
    
    def read_query_df(self, query):
        return pd.read_sql(query, self.engine)


# ------------------------------------------------------------------------------