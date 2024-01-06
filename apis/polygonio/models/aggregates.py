import pandas as pd
import pytz
from datetime import datetime
import asyncpg
from asyncpg.exceptions import InvalidCatalogNameError
import os

from dataclasses import dataclass, asdict
from typing import List


@dataclass
class AggregatesData:
    def __init__(self, response_data):
        ticker: str
        open: float
        high: float
        low: float
        close: float
        timestamp: str

    
        utc_tz = pytz.timezone('UTC')
        central_tz = pytz.timezone('America/Chicago')
        self.pool = None
        self.conn = os.environ.get('AGGREGATES_STRING')
        self.user = os.environ.get('DB_USER')
        self.host = "localhost"
        self.port = 5432

        self.adjusted = response_data.get("adjusted")
        self.next_url = response_data.get("next_url")
        self.ticker = response_data.get("ticker")
        self.query_count = response_data.get("queryCount")
        self.request_id = response_data.get("request_id")
        self.results = response_data.get("results")
        self.db_config = { 
            'user': os.environ.get('DB_USER'),
            'host': os.environ.get('DB_HOST'),
            'password': os.environ.get('DB_PASSWORD'),
            'port': os.environ.get('DB_PORT'),
            'name': os.environ.get('AGGS_DB')

        }
        if self.results is None:
            self.close = []
            self.high = []
            self.low = []
            self.n = []
            self.open = []
            self.timestamp = []
            self.volume = []
            self.volume_weighted_average = []
        else:
            self.close = [i['c'] for i in self.results]
            self.high = [i['h'] for i in self.results]
            self.low = [i['l'] for i in self.results]
      
            self.open = [i['o'] for i in self.results]
            self.timestamp = [datetime.utcfromtimestamp(i['t'] / 1000).replace(tzinfo=utc_tz).astimezone(central_tz).replace(tzinfo=None) for i in self.results]

            self.volume = [i['v'] for i in self.results] if not self.ticker.startswith("I:") else None


            self.data_dict = { 
                'Open': self.open,
                'High': self.high,
                'Low': self.low,
                'Close': self.close,
                
                'Timestamp': self.timestamp

            }


            self.df = pd.DataFrame(self.data_dict)

    def __str__(self):
        return f"AggregatesData({self.ticker})"

    def __repr__(self):
        return self.__str__()
    
    def to_db_format(self):
        return [(self.ticker, o, h, l, c, t) for o, h, l, c, t in zip(self.open, self.high, self.low, self.close, self.timestamp)]

    async def create_pool(self, db_config):
        """
        Create a connection pool using the given database configuration.

        Parameters:
        - db_config (dict): Database configuration parameters including user, password, database, host, port.

        Returns:
        - asyncpg.Pool: A connection pool.
        """
        self.pool = await asyncpg.create_pool(
            user=db_config['DB_USER'],
            password=db_config['DB_PASSWORD'],
            database=db_config['DB_NAME'],
            host=db_config['DB_HOST'],
            port=db_config['DB_PORT']
        )
        return self.pool

    # Function to create PostgreSQL table
  
    async def connect(self, conn_str):
        """
        Create a single connection to the database using the given connection string.

        Parameters:
        - conn_str (str): The database connection string.

        Returns:
        - asyncpg.Connection: A single database connection.
        """
        try:
            self.conn = await asyncpg.connect(conn_str)
        except InvalidCatalogNameError:
            print(f'Database does not exist! Attempting to create database...')
            
            # Extract database name from the connection string
            db_name = conn_str.split('/')[-1].split('?')[0]
            
            # Create a temporary connection string without the database name
            temp_conn_str = conn_str.replace(f"/{db_name}", "/postgres")
            
            # Connect to the default "postgres" database to create a new database
            temp_conn = await asyncpg.connect(temp_conn_str)
            
            try:
                await temp_conn.execute(f'CREATE DATABASE {db_name}')
                print(f"Database {db_name} created successfully.")
                
                # Now connect to the newly created database
                self.conn = await asyncpg.connect(conn_str)
                
            except Exception as e:
                print(f"Failed to create database. Error: {e}")
            finally:
                await temp_conn.close()
                
        return self.conn


    async def create_table(self):
    
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                CREATE TABLE IF NOT EXISTS aggregates_data (
                    ticker VARCHAR(10),
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    timestamp TIMESTAMPTZ
                );
                """)
    def to_dict(self):
        return asdict(self)
    
    #function to insert into Postgres
    async def insert_data(self, data_dicts: List[dict]):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for data_dict in data_dicts:
                    await conn.execute("""
                    INSERT INTO aggregates_data (ticker, open, high, low, close, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6);
                    """, data_dict['Ticker'], data_dict['Open'], data_dict['High'], data_dict['Low'], data_dict['Close'], data_dict['Timestamp'])