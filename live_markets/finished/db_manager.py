import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()
import asyncpg
from asyncpg.exceptions import UniqueViolationError, ForeignKeyViolationError, DataError
import asyncpg
import uuid
import os
from typing import List
from datetime import datetime
from asyncpg import create_pool
from datetime import date
import json
import asyncio
import pandas as pd
import numpy as np
from asyncpg.exceptions import UniqueViolationError, ForeignKeyViolationError
import pytz
from asyncio import Lock
from datetime import timezone
lock = Lock()
password = os.environ.get('DB_PASSWORD') # Default password if not found in environment variables
db_config_fudstop = {
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": 'chuck', # Default to 'postgres' if 'DB_USER' not found
    "password": password, # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME') # Database name for the new jawless database
}

db_options_chat_config =  { 
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER'), # Default to 'postgres' if 'DB_USER' not found
    "password": password, # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME') # Database name for the new jawless database
}
class DbManager:
    def __init__(self, host, port, user, password, database):
        self.conn = None
        self.pool = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        print(self.connection_string)
        print(self.user)
        self.chat_memory = []  # In-memory list to store chat messages

    async def connect(self):
        self.pool = await create_pool(
            host='localhost', user='chuck', database='markets', port=5432, password='fud', min_size=1, max_size=10
        )

        return self.pool
    
    async def get_ticker_id_by_symbol(self, option_symbol):
        async with self.pool.acquire() as connection:
            query = "SELECT ticker_id FROM webull_options WHERE symbol = $1"
            ticker_id = await connection.fetchval(query, option_symbol)
            
            # Check if ticker_id was found
            if ticker_id is not None:
                return (True, ticker_id)
            else:
                return (False, None)

    async def insert_specials(self, ticker, strike, call_put, expiry, buy_pct, neutral_pct, sell_pct,
                                official_open, last_price, day_vwap, buy_volume, neutral_volume, sell_volume,
                                last_volume, avg_price, volume_percent_total, total_trades, moneyness,
                                price_diff, price_change_pct, volume_to_price_ratio, option_id):
        conn = await self.connect()
        query = """
        INSERT INTO volanal (ticker, strike, call_put, expiry, buy_pct, neutral_pct, sell_pct,
                                official_open, last_price, day_vwap, buy_volume, neutral_volume, sell_volume,
                                last_volume, avg_price, volume_percent_total, total_trades, moneyness,
                                price_diff, price_change_pct, volume_to_price_ratio, option_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        """
        await conn.execute(query, ticker, strike, call_put, expiry, buy_pct, neutral_pct, sell_pct,
                            official_open, last_price, day_vwap, buy_volume, neutral_volume, sell_volume,
                            last_volume, avg_price, volume_percent_total, total_trades, moneyness,
                            price_diff, price_change_pct, volume_to_price_ratio, option_id)

    async def options_chat_connect(self):

        self.pool = await create_pool(
            **db_options_chat_config, min_size=1, max_size=20
        )

        return self.pool
    async def insert_chat_history(self, sender: str, message: str, session_id: uuid.UUID, connection_string: str):
        conn = await asyncpg.connect(connection_string)
        query = """INSERT INTO chat_history (sender, message, session_id, created_at) VALUES ($1, $2, $3, $4);"""
        await conn.execute(query, sender, message, session_id, datetime.now())
   

    async def batch_insert_ticker_snaps(self, records: list):
        async with self.pool.acquire() as conn:
            insert_query = '''
            INSERT INTO ticker_snapshots (
                underlying_ticker, day_close, day_open, day_low, day_high, day_volume,
                day_vwap, prev_close, prev_high, prev_low, prev_open, prev_volume,
                prev_vwap, change_percent, change, min_high, min_low, min_open,
                min_close, min_volume, min_vwap, min_accumulated_volume, last_trade_size,
                last_trade_price, last_trade_conditions, last_trade_exchange, last_trade_timestamp,
                ask, ask_size, bid, bid_size, quote_timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33);
            '''
            await conn.executemany(insert_query, records)
  
    async def execute_query_and_fetch_one(self, query, parameters=None):
        async with self.pool.acquire() as conn:
            if parameters:
                row = await conn.fetchrow(query, *parameters)
            else:
                row = await conn.fetchrow(query)
            return row
    async def check_if_exists(self, link, table_name):
        query = f"SELECT COUNT(*) FROM {table_name} WHERE link = ?"
        count = await self.execute_query_and_fetch_one(query, (link,))
        return count[0] > 0


    async def insert_td9_state(self, ticker: str, status: str, timespan: str):
        async with self.pool.acquire() as connection:
            try:
                await connection.execute(
                    """
                    INSERT INTO td9_states (ticker, status, timespan) VALUES ($1, $2, $3);
                    """,
                    ticker, status, timespan
                )
            except UniqueViolationError:
                print(f"SKIPPING {ticker} {status} {timespan}")
    async def fetch(self, query):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return records
    async def clear_old_td9_states(self):
        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                DELETE FROM td9_states WHERE timestamp <= (current_timestamp - interval '4 minutes');
                """
            )

    async def process_response(self, response):
        async with self.pool.acquire() as conn:
            for result in response['results']:
                # Convert Unix Msec timestamp to datetime in Eastern timezone
                timestamp = result['t'] / 1000  # Convert milliseconds to seconds
                timestamp_eastern = timestamp.astimezone(timezone('US/Eastern'))
                
                # Map single-letter columns to their full names
                result['open'] = result.pop('o')
                result['high'] = result.pop('h')
                result['low'] = result.pop('l')
                result['close'] = result.pop('c')
                result['volume'] = result.pop('v')

                # Insert data into the table
                await conn.execute('''
                    INSERT INTO stock_data (ticker, adjusted, query_count, request_id, results_count,
                                            status, open, high, low, n, close, otc, t, volume, vw)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ''', (response['ticker'], response['adjusted'], response['queryCount'], response['request_id'],
                      response['resultsCount'], response['status'], result['open'], result['high'], result['low'],
                      result['n'], result['close'], result['otc'], timestamp_eastern, result['volume'], result['vw']))

    async def disconnect(self):
        await self.pool.close()
    async def safe_batch_insert_dataframe(self, df, table_name, unique_columns, retries=3):
        for i in range(retries):
            try:
                await self.batch_insert_dataframe(df, table_name, unique_columns)
                break  # Successful, break the loop
            except Exception as e:
                print(f"An error occurred: {e}")
                if i < retries - 1:
                    print("Retrying...")
                else:
                    print("Max retries reached. Raising the exception.")
                    raise

    async def insert_symbol(self, symbol):
        async with self.pool.acquire() as connection:
            insert_query = '''
            INSERT INTO symbols (symbol)
            VALUES ($1)
            ON CONFLICT (symbol) DO NOTHING;
            '''
            await connection.execute(insert_query, symbol)

    async def insert_sma_data(self, records):
        # records is a list of tuples, where each tuple corresponds to a row to be inserted
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO sma_data (sma_value, timestamp, sma_length, ticker, timespan, something)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (ticker, timestamp, timespan) DO UPDATE
                SET sma_value = EXCLUDED.sma_value,
                    sma_length = EXCLUDED.sma_length,
                    something = EXCLUDED.something
                """,
                records
            )
    async def insert_options_snapshot(self, data_dict):
        async with self.pool.acquire() as connection:
            insert_query = '''
            INSERT INTO options_data (name, option_symbol, ticker, strike, "call put", expiry, "underlying price",
            "change", "change percent", "early change", "early change_percent", "change to break_even", "break even price",
            open, high, low, close, "previous close", volume, oi, iv, delta, gamma, theta, vega, "trade size", "trade price",
            "trade exchange", "trade conditions", "trade timestamp", ask, "ask size", "ask exchange", bid, "bid size", "bid exchange")
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
            $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36)
            '''
            await connection.execute(insert_query, *data_dict.values())
    async def insert_tickers(self, ticker_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare the SQL INSERT query
                insert_query = '''
                INSERT INTO tickers (ticker_symbol, todays_change, todays_change_perc)
                VALUES ($1, $2, $3)
                ON CONFLICT (ticker_symbol) DO UPDATE 
                SET todays_change = EXCLUDED.todays_change,
                    todays_change_perc = EXCLUDED.todays_change_perc;
                '''
                
                # Execute the query in a batch
                await connection.executemany(insert_query, [(d['ticker'], d['todaysChange'], d['todaysChangePerc']) for d in ticker_data])

    async def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(query)
        return exists
    


    async def create_dynamic_view(self, table_name, features_dict):
        feature_expressions = ", ".join([
            f"({expression}) as {feature_name}"
            for feature_name, expression in features_dict.items()
        ])
        create_view_query = f"""
        CREATE OR REPLACE VIEW {table_name}_with_features AS
        SELECT *, {feature_expressions}
        FROM {table_name};
        """

        async with self.pool.acquire() as connection:
            await connection.execute(create_view_query)


    async def insert_embedding(self, original_text, embedding):
        query = """
        INSERT INTO embeddings (original_text, embedding)
        VALUES ($1, $2);
        """
        async with self.pool.acquire() as connection:
            await connection.execute(query, original_text, embedding)

    async def get_embedding(self, original_text):
        query = """
        SELECT embedding FROM embeddings
        WHERE original_text = $1;
        """
        async with self.pool.acquire() as connection:
            return await connection.fetchval(query, original_text)

    async def create_table(self, df, table_name, unique_column):
        print("Connected to the database.")
        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64': 'TIMESTAMP',
            'datetime64[ns]': 'timestamp',
            'datetime64[ms]': 'timestamp',
            'datetime64[ns, US/Eastern]': 'TIMESTAMP WITH TIME ZONE'
        }
        print(f"DataFrame dtypes: {df.dtypes}")
        # Check for large integers and update dtype_mapping accordingly
        for col, dtype in zip(df.columns, df.dtypes):
            if dtype == 'int64':
                max_val = df[col].max()
                min_val = df[col].min()
                if max_val > 2**31 - 1 or min_val < -2**31:
                    dtype_mapping['int64'] = 'BIGINT'
        history_table_name = f"{table_name}_history"
        async with self.pool.acquire() as connection:

            table_exists = await connection.fetchval(f"SELECT to_regclass('{table_name}')")
            
            if table_exists is None:
                unique_constraint = f'UNIQUE ({unique_column})' if unique_column else ''
                create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(f'"{col}" {dtype_mapping[str(dtype)]}' for col, dtype in zip(df.columns, df.dtypes))},
                    "insertion_timestamp" TIMESTAMP,
                    {unique_constraint}
                )
                """
                print(f"Creating table with query: {create_query}")

                # Create the history table
                history_create_query = f"""
                CREATE TABLE IF NOT EXISTS {history_table_name} (
                    id serial PRIMARY KEY,
                    operation CHAR(1) NOT NULL,
                    changed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                    {', '.join(f'"{col}" {dtype_mapping[str(dtype)]}' for col, dtype in zip(df.columns, df.dtypes))}
                );
                """
                print(f"Creating history table with query: {history_create_query}")
                await connection.execute(history_create_query)
                try:
                    await connection.execute(create_query)
                    print(f"Table {table_name} created successfully.")
                except asyncpg.UniqueViolationError as e:
                    print(f"Unique violation error: {e}")
            else:
                print(f"Table {table_name} already exists.")
            
            # Create the trigger function
            trigger_function_query = f"""
            CREATE OR REPLACE FUNCTION save_to_{history_table_name}()
            RETURNS TRIGGER AS $$
            BEGIN
                INSERT INTO {history_table_name} (operation, changed_at, {', '.join(f'"{col}"' for col in df.columns)})
                VALUES (
                    CASE
                        WHEN (TG_OP = 'DELETE') THEN 'D'
                        WHEN (TG_OP = 'UPDATE') THEN 'U'
                        ELSE 'I'
                    END,
                    current_timestamp,
                    {', '.join('OLD.' + f'"{col}"' for col in df.columns)}
                );
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """
            await connection.execute(trigger_function_query)

            # Create the trigger
            trigger_query = f"""
            DROP TRIGGER IF EXISTS tr_{history_table_name} ON {table_name};
            CREATE TRIGGER tr_{history_table_name}
            AFTER UPDATE OR DELETE ON {table_name}
            FOR EACH ROW EXECUTE FUNCTION save_to_{history_table_name}();
            """
            await connection.execute(trigger_query)


            # Alter existing table to add any missing columns
            for col, dtype in zip(df.columns, df.dtypes):
                alter_query = f"""
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE {table_name} ADD COLUMN "{col}" {dtype_mapping[str(dtype)]};
                    EXCEPTION
                        WHEN duplicate_column THEN
                        NULL;
                    END;
                END $$;
                """
                await connection.execute(alter_query)

    async def insert_dataframe(self, df, table_name, unique_columns):
        # Check if the table already exists
        if not await self.table_exists(table_name):
            await self.create_table(df, table_name, unique_columns)  # Assuming this function exists

        # Convert DataFrame to list of records
        records = df.to_records(index=False)
        data = list(records)

        # Get a connection from the connection pool
        async with self.pool.acquire() as connection:
            # Fetch the PostgreSQL table schema to get column types (Only once)
            column_types = await connection.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'")
            column_type_dict = {item['column_name']: item['data_type'] for item in column_types}

            # Create a transaction
            async with connection.transaction():
                # Prepare the INSERT query
                insert_query = f"""
                INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in df.columns)}) 
                VALUES ({', '.join('$' + str(i) for i in range(1, len(df.columns) + 1))})
                ON CONFLICT ({unique_columns})
                DO UPDATE SET {', '.join(f'"{col}" = excluded."{col}"' for col in df.columns)}
                """

                async def execute_query(record):
                    new_record = []
                    for col, val in zip(df.columns, record):
                        pg_type = column_type_dict.get(col, None)

                        # Conversion logic starts here
                        if val is None:
                            new_record.append(None)
                        elif pg_type == 'timestamp' and isinstance(val, np.datetime64):
                            print(f"Converting {val} to datetime")  # Debugging print statement
                            new_record.append(pd.Timestamp(val).to_pydatetime())
                        elif isinstance(val, np.datetime64) and np.isnat(val):
                            new_record.append(None)

                        elif pg_type == 'timestamp without time zone' and isinstance(val, np.datetime64):
                            new_record.append(pd.Timestamp(val).to_pydatetime() if pd.notnull(val) else None)
                        elif pg_type == 'timestamp with time zone' and isinstance(val, np.datetime64):
                            new_record.append(pd.Timestamp(val).to_pydatetime() if pd.notnull(val) else None)
                        elif pg_type in ['double precision', 'real']:
                            if isinstance(val, str):
                                new_record.append(float(val))
                            elif isinstance(val, pd.Timestamp):
                                # Convert to datetime in eastern timezone or whatever you need to do
                                val = val.tz_convert('US/Eastern')
                                new_record.append(val)
                            else:
                                new_record.append(float(val))
                        elif pg_type == 'integer' and not isinstance(val, int):
                            new_record.append(int(val))
                        else:
                            new_record.append(val)
                        # Conversion logic ends here


                    try:
                        await connection.execute(insert_query, *new_record)
                    except Exception as e:
                        print(f"An error occurred while inserting the record: {e}")
                        await connection.execute('ROLLBACK')
                        raise

                # Execute the query for each record concurrently
                await asyncio.gather(*(execute_query(record) for record in data))








    async def batch_insert_dataframe(self, df, table_name, unique_columns, batch_size=250):
        async with lock:
            if not await self.table_exists(table_name):
                await self.create_table(df, table_name, unique_columns)

            # Debug: Print DataFrame columns before modifications
            #print("Initial DataFrame columns:", df.columns.tolist())
            
            df = df.copy()
            df.dropna(inplace=True)
            df['insertion_timestamp'] = [datetime.now() for _ in range(len(df))]

            # Debug: Print DataFrame columns after modifications
            #print("Modified DataFrame columns:", df.columns.tolist())
            
            records = df.to_records(index=False)
            data = list(records)


            async with self.pool.acquire() as connection:
                column_types = await connection.fetch(
                    f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
                )
                type_mapping = {col: next((item['data_type'] for item in column_types if item['column_name'] == col), None) for col in df.columns}

                async with connection.transaction():
                    insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in df.columns)}) 
                    VALUES ({', '.join('$' + str(i) for i in range(1, len(df.columns) + 1))})
                    ON CONFLICT ({unique_columns})
                    DO UPDATE SET {', '.join(f'"{col}" = excluded."{col}"' for col in df.columns)}
                    """
            
                    batch_data = []
                    for record in data:
                        new_record = []
                        for col, val in zip(df.columns, record):
                   
                            pg_type = type_mapping[col]

                            if val is None:
                                new_record.append(None)
                            elif pg_type == 'timestamp' and isinstance(val, np.datetime64):
                                new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))

            
                            elif isinstance(val, datetime):
                                new_record.append(pd.Timestamp(val).to_pydatetime())
                            elif pg_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone'] and isinstance(val, np.datetime64):
                                new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))  # Modified line
                            elif pg_type in ['double precision', 'real'] and not isinstance(val, str):
                                new_record.append(float(val))
                            elif isinstance(val, np.int64):  # Add this line to handle numpy.int64
                                new_record.append(int(val))
                            elif pg_type == 'integer' and not isinstance(val, int):
                                new_record.append(int(val))
                            else:
                                new_record.append(val)
                    
                        batch_data.append(new_record)

                        if len(batch_data) == batch_size:
                            try:
                                
                             
                                await connection.executemany(insert_query, batch_data)
                                batch_data.clear()
                            except Exception as e:
                                print(f"An error occurred while inserting the record: {e}")
                                await connection.execute('ROLLBACK')
                                raise

                if batch_data:  # Don't forget the last batch
       
                    try:

                        await connection.executemany(insert_query, batch_data)
                    except Exception as e:
                        print(f"An error occurred while inserting the record: {e}")
                        await connection.execute('ROLLBACK')
                        raise
    async def save_to_history(self, df, main_table_name, history_table_name):
        # Assume the DataFrame `df` contains the records to be archived
        if not await self.table_exists(history_table_name):
            await self.create_table(df, history_table_name, None)

        df['archived_at'] = datetime.now()  # Add an 'archived_at' timestamp
        await self.batch_insert_dataframe(df, history_table_name, None)
