import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()
import asyncpg
from asyncpg.exceptions import UniqueViolationError, ForeignKeyViolationError, DataError
from apis.polygonio.mapping import stock_condition_dict, option_condition_dict, STOCK_EXCHANGES, OPTIONS_EXCHANGES
import asyncpg
import uuid
import os
from typing import List
from datetime import datetime
from asyncpg.exceptions import DatatypeMismatchError
from asyncpg import create_pool
from datetime import date
from email.utils import parsedate_to_datetime
import json
import asyncio
import pandas as pd
import numpy as np
from asyncpg.exceptions import UniqueViolationError, ForeignKeyViolationError
import pytz
from asyncio import Lock
from datetime import timezone
lock = Lock()
db_config_charlie = {
    "host": 'localhost', # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": 'chuck', # Default to 'postgres' if 'DB_USER' not found
    "password": "fud", # Use the password from environment variable or default
    "database": "markets" # Database name for the new jawless database
}

db_options_chat_config =  { 
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": 5432, # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('chuck'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('fud'), # Use the password from environment variable or default
    "database": "markets" # Database name for the new jawless database
}
class DatabaseManager:
    def __init__(self, host, port, user, password, database):
        self.conn = None
        self.pool = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        print(self.database)
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        print(self.connection_string)
        self.chat_memory = []  # In-memory list to store chat messages

    async def connect(self):
        self.pool = await create_pool(
            **db_config_charlie, min_size=1, max_size=10
        )

        return self.pool
    

    async def options_chat_connect(self):
        self.pool = await create_pool(
            **db_options_chat_config, min_size=1, max_size=20
        )

        return self.pool
    async def insert_chat_history(self, sender: str, message: str, session_id: uuid.UUID, connection_string: str):
        conn = await asyncpg.connect(connection_string)
        query = """INSERT INTO chat_history (sender, message, session_id, created_at) VALUES ($1, $2, $3, $4);"""
        await conn.execute(query, sender, message, session_id, datetime.now())
        await conn.close()

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
            await conn.close()
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

    async def insert_equity_agg(self, data: dict):
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO equity_agg (type, stock_symbol, close_price, high_price, low_price, open_price, volume, official_open, accumulated_volume, vwap_price, agg_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            """, data['type'], data['stock_symbol'], data['close_price'], data['high_price'], data['low_price'], data['open_price'], data['volume'], data['official_open'], data['accumulated_volume'], data['vwap_price'], data['agg_timestamp'])

    async def insert_equity_trade(self, data: dict):
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO equity_trade (type, stock_symbol, trade_exchange, trade_price, trade_size, trade_conditions, trade_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, data['type'], data['stock_symbol'], data['trade_exchange'], data['trade_price'], data['trade_size'], data['trade_conditions'], data['trade_timestamp'])

    async def insert_equity_quote(self, data: dict):
        async with self.pool.acquire() as conn:
            await conn.execute("""
            INSERT INTO equity_quote (type, quote_symbol, ask, bid, ask_size, bid_size, indicator, condition, ask_exchange, bid_exchange, timestamp, tape)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, data['type'], data['quote_symbol'], data['ask'], data['bid'], data['ask_size'], data['bid_size'], data['indicator'], data['condition'], data['ask_exchange'], data['bid_exchange'], data['timestamp'], data['tape'])
    async def insert_option_symbol(self, underlying_ticker, call_put, strike, expiry, option_symbol):
        try:
            query = """
            INSERT INTO option_symbols (underlying_ticker, call_put, strike, expiry, option_symbol)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (underlying_ticker, call_put, strike, expiry)
            DO UPDATE SET count = option_symbols.count + 1;
            """
            expiry = datetime.strptime(expiry, '%m/%d/%Y').date()
            # Execute the query with the provided values
            async with self.pool.acquire() as conn:
                await conn.execute(query, underlying_ticker, call_put, strike, expiry, option_symbol)

        except Exception as e:
            print("An error occurred:", e)

    async def save_structured_message(self, data: dict, table_name: str):
        fields = ', '.join(data.keys())
        values = ', '.join([f"${i+1}" for i in range(len(data))])
        
        query = f'INSERT INTO {table_name} ({fields}) VALUES ({values})'
        print(self.connection_string)
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(query, *data.values())
            except UniqueViolationError:
                print('Duplicate - SKipping')


    async def insert_indices_aggs(self, data_list):
        sql = """
        INSERT INTO indices_aggs (symbol, name, official_open, minute_open, minute_high, minute_low, minute_close, start_time, end_time) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """

        async with self.pool.acquire() as conn:
            # Use a transaction for batch insert
            async with conn.transaction():
                await conn.executemany(sql, [(d['symbol'], d['name'], d['official_open'], d['minute_open'], d['minute_high'], d['minute_low'], d['minute_close'], d['start_time'], d['end_time']) for d in data_list])
    async def insert_summary(self, url, summary, question, doc_type):
        async with self.pool.acquire() as conn:
            try:
                await conn.execute('''
                    INSERT INTO summaries (url, summary, question, doc_type) VALUES ($1, $2, $3, $4)
                    ON CONFLICT (url) DO UPDATE SET summary = EXCLUDED.summary;
                ''', url, summary, question, doc_type)
            except Exception as e:
                print(f"An error occurred: {e}")
 
    async def insert_options_data(self, data_list):
        query = '''
        INSERT INTO option_snaps (
            last_trade_timestamp, option_symbol, expiry, strike, call_put,
            underlying_symbol, underlying_price, change_to_breakeven,
            trade_size, trade_price, trade_conditions, style, sip_timestamp,
            trade_exchange, break_even_price, implied_volatility, open_interest,
            name, open, high, low, close, volume, prev_close, midpoint,
            early_trading_change_percent, change, early_trading_change, change_percent,
            delta, vega, theta, gamma, ask, bid, ask_size, bid_size, ask_exchange, bid_exchange
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
            $29, $30, $31, $32, $33, $34, $35, $36
        )
        ON CONFLICT (underlying_symbol, strike, expiry, call_put)
        DO UPDATE SET
            last_trade_timestamp = EXCLUDED.last_trade_timestamp,
            underlying_price = EXCLUDED.underlying_price,
            change_to_breakeven = EXCLUDED.change_to_breakeven,
            trade_size = EXCLUDED.trade_size,
            trade_price = EXCLUDED.trade_price,
            trade_conditions = EXCLUDED.trade_conditions,
            style = EXCLUDED.style,
            sip_timestamp = EXCLUDED.sip_timestamp,
            trade_exchange = EXCLUDED.trade_exchange,
            break_even_price = EXCLUDED.break_even_price,
            implied_volatility = EXCLUDED.implied_volatility,
            open_interest = EXCLUDED.open_interest,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            prev_close = EXCLUDED.prev_close,
            early_trading_change_percent = EXCLUDED.early_trading_change_percent,
            change = EXCLUDED.change,
            early_trading_change = EXCLUDED.early_trading_change,
            change_percent = EXCLUDED.change_percent,
            delta = EXCLUDED.delta,
            vega = EXCLUDED.vega,
            theta = EXCLUDED.theta,
            gamma = EXCLUDED.gamma,
            ask = EXCLUDED.ask,
            bid = EXCLUDED.bid,
            ask_size = EXCLUDED.ask_size,
            bid_size = EXCLUDED.bid_size,
            midpoint = EXCLUDED.midpoint,
            ask_exchange = EXCLUDED.ask_exchange,
            bid_exchange = EXCLUDED.bid_exchange
        '''

        # Prepare records in the format suitable for batch inserting
        # Prepare records in the format suitable for batch inserting
        # Transpose the list of values for each column into rows
        transposed_data = zip(*data_list.values())

        # Prepare records in the format suitable for batch inserting
        records = [tuple(row) for row in transposed_data]

       
        # Perform batch insert
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, records)



    async def insert_option_snaps(self, data_dict):
        
        query = """
        INSERT INTO option_snaps (
            last_trade_timestamp,
            option_symbol,
            expiry,
            strike,
            call_put,
            underlying_symbol,
            underlying_price,
            change_to_breakeven,
            trade_size,
            trade_price,
            trade_conditions,
            sip_timestamp,
            trade_exchange,
            break_even_price,
            implied_volatility,
            open_interest,
            name,
            open,
            high,
            low,
            close,
            volume,
            prev_close,
            early_trading_change_percent,
            change,
            early_trading_change,
            change_percent,
            delta,
            vega,
            theta,
            gamma,
            ask,
            bid,
            ask_size,
            bid_size,
            ask_exchange,
            bid_exchange
        )
        VALUES(
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
            $31, $32, $33, $34, $35, $36, $37
        )
        ON CONFLICT (underlying_symbol, call_put, strike, expiry, option_symbol)
        DO NOTHING;
        """
        try:
            if data_dict.get('expiry'):  # Check if 'expiry' exists and is not None
                try:
                    data_dict['expiry'] = datetime.strptime(data_dict['expiry'], '%Y-%m-%d').date()
                except ValueError:
                    print(f"Invalid date format for expiry: {data_dict['expiry']}")
      
            else:
                print("Expiry is None, skipping this record.")

            data_dict['expiry'] = datetime.strptime(data_dict['expiry'], '%Y-%m-%d').date()
            if data_dict['expiry'] is not None:
                await self.pool.execute(
                    query,
                    data_dict['sip_timestamp'],  # Assuming 'sip_timestamp' is equivalent to 'last_trade_timestamp' in the database
                    data_dict['option_symbol'],
                    data_dict['expiry'],
                    data_dict['strike'],
                    data_dict['call_put'],
                    data_dict['underlying_symbol'],
                    data_dict['underlying_price'],
                    data_dict['change_to_breakeven'],
                    data_dict['trade_size'],
                    data_dict['trade_price'],
                    data_dict['trade_conditions'],
                    data_dict['sip_timestamp'],
                    data_dict['trade_exchange'],
                    data_dict['break_even_price'],
                    data_dict['implied_volatility'],
                    data_dict['open_interest'],
                    data_dict['name'],
                    data_dict['open'],
                    data_dict['high'],
                    data_dict['low'],
                    data_dict['close'],
                    data_dict['volume'],
                    data_dict['prev_close'],
                    data_dict['early_trading_change_percent'],
                    data_dict['change'],
                    data_dict['early_trading_change'],
                    data_dict['change_percent'],
                    data_dict['delta'],
                    data_dict['vega'],
                    data_dict['theta'],
                    data_dict['gamma'],
                    data_dict['ask'],
                    data_dict['bid'],
                    data_dict['ask_size'],
                    data_dict['bid_size'],
                    data_dict['ask_exchange'],
                    data_dict['bid_exchange']
                )
        except TypeError:
            print('None found - skipping')


    async def insert_option_trades(self, trade_data_list):
        query = """
        INSERT INTO option_trades (option_symbol, underlying_symbol,strike, call_put, expiry, conditions, exchange, size, price, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (underlying_symbol, strike, call_put, expiry)
        DO UPDATE SET
            conditions = EXCLUDED.conditions,
            exchange = EXCLUDED.exchange,
            size = EXCLUDED.size,
            price = EXCLUDED.price,
            timestamp = EXCLUDED.timestamp;
        """
        
        records = [
            (
                trade['option_symbol'], trade['underlying_symbol'], trade['strike'], trade['call_put'], trade['expiry'], trade['conditions'], trade['exchange'], trade['size'], trade['price'], trade['timestamp']
            )
            for trade in trade_data_list
        ]
        
        async with self.pool.acquire() as conn:
            try:
                await conn.executemany(query, records)
            except DataError:
                print(f"Data error for - {records[0]}")

    async def batch_insert_option_trades(self, records):
        query = """
        INSERT INTO option_trades (option_symbol, underlying_symbol, strike, call_put, expiry, conditions, exchange, size, price, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (underlying_symbol, strike, call_put, expiry)
        DO UPDATE SET conditions = EXCLUDED.conditions, exchange = EXCLUDED.exchange, size = EXCLUDED.size, price = EXCLUDED.price, timestamp = EXCLUDED.timestamp;
        """
        # Convert list of dictionaries to list of tuples
        records_as_tuples = [(
            record['option_symbol'],
            record['underlying_symbol'],
            record['strike'],
            record['call_put'],
            record['expiry'],
            record['conditions'],
            record['exchange'],
            record['size'],
            record['price'],
            record['timestamp']
        ) for record in records]
        
        try:
            await self.pool.executemany(query, records_as_tuples)
        except Exception as e:
            print(f"Failed to insert batch into database: {e}")
    async def fetch_iter(self, query, *params):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor(query):
                    yield record

    async def insert_bb_dps(self, ticker, price, size, notional_value, sector, message_id, timestamp):
        query = '''
        INSERT INTO bb_dps (ticker, price, size, notional_value, sector, message_id, timestamp)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (message_id) DO NOTHING;
        '''
        
        async with self.pool.acquire() as conn:
            await conn.execute(query, ticker, price, size, notional_value, sector, message_id, timestamp)
    async def insert_option_snaps(self, data_dicts):
        # Create a connection pool if it doesn't exist
        
        async with self.pool.acquire() as conn:
            for data_dict in data_dicts:
                try:
                    data_dict['expiry'] = datetime.strptime(data_dict['expiry'], '%Y-%m-%d').date()
                except TypeError:
                    continue
                try:
                    await conn.execute('''
                        INSERT INTO option_snaps (
                            name, option_symbol, underlying_symbol, strike, call_put, expiry,
                            underlying_price, change, change_percent, early_change, early_change_percent,
                            change_to_break_even, break_even_price, open, high, low, close,
                            previous_close, volume, oi, iv, delta, gamma, theta, vega,
                            trade_size, trade_price, trade_exchange, trade_conditions, trade_timestamp,
                            ask, ask_size, ask_exchange, bid, bid_size, bid_exchange
                        ) VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                            $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                            $27, $28, $29, $30, $31, $32, $33, $34, $35, $36
                        );
                    ''', *data_dict.values())
                except Exception as e:
                    print(f"Failed to insert record {data_dict}. Error: {e}")

    async def insert_day_data(self, day_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare SQL queries
                lookup_query = 'SELECT ticker_id FROM tickers WHERE ticker_symbol = $1;'
                insert_query = '''
                INSERT INTO day (ticker_id, c, h, l, o, v, vw)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (ticker_id) DO UPDATE 
                SET c = EXCLUDED.c,
                    h = EXCLUDED.h,
                    l = EXCLUDED.l,
                    o = EXCLUDED.o,
                    v = EXCLUDED.v,
                    vw = EXCLUDED.vw,
                    insertion_timestamp = CURRENT_TIMESTAMP;  -- Update timestamp on conflict
                '''
                
                # Loop through each day_data entry
                for d in day_data:
                    # Look up the ticker_id for the given ticker_symbol
                    ticker_id = await connection.fetchval(lookup_query, d['ticker_symbol'])
                    
                    # Insert data into the day table
                    if ticker_id is not None:
                        await connection.execute(insert_query, ticker_id, d['c'], d['h'], d['l'], d['o'], d['v'], d['vw'])

    async def insert_last_quote_data(self, last_quote_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare SQL queries
                lookup_query = 'SELECT ticker_id FROM tickers WHERE ticker_symbol = $1;'
                insert_query = '''
                INSERT INTO last_quote (ticker_id, "P", "S", p, s, t)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (ticker_id) DO UPDATE 
                SET "P" = EXCLUDED.P,
                    "S" = EXCLUDED.S,
                    p = EXCLUDED.p,
                    s = EXCLUDED.s,
                    t = EXCLUDED.t;
                '''
                
                # Loop through each last_quote_data entry
                for d in last_quote_data:
                    # Look up the ticker_id for the given ticker_symbol
                    ticker_id = await connection.fetchval(lookup_query, d['ticker_symbol'])
                    
                    if d['t']:
                        timestamp = datetime.fromtimestamp(d['t'] / 1e9)
                    # Insert data into the last_quote table
                    if ticker_id is not None:
                        await connection.execute(insert_query, ticker_id, d['P'], d['S'], d['p'], d['s'], timestamp)
    async def insert_last_trade_data(self, last_trade_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare SQL queries
                lookup_query = 'SELECT ticker_id FROM tickers WHERE ticker_symbol = $1;'
                insert_query = '''
                INSERT INTO last_trade (ticker_id, c, i, p, s, t, x)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (ticker_id) DO UPDATE 
                SET c = EXCLUDED.c,
                    i = EXCLUDED.i,
                    p = EXCLUDED.p,
                    s = EXCLUDED.s,
                    t = EXCLUDED.t,
                    x = EXCLUDED.x;
                '''
                
                # Loop through each last_trade_data entry
                for d in last_trade_data:
                    if d['t']:
                        timestamp = datetime.fromtimestamp(d['t'] / 1e9)
                    if d['x']:
                        exchange = STOCK_EXCHANGES.get(d['x'])
                    if d['c']:
                        conditions = [stock_condition_dict.get(i) for i in d['c']] if d['c'] is not None else None
                        # Look up the ticker_id for the given ticker_symbol
                        ticker_id = await connection.fetchval(lookup_query, d['ticker_symbol'])
                        
                        # Insert data into the last_trade table
                        if ticker_id is not None and conditions is not None:
                            await connection.execute(insert_query, ticker_id, conditions, d['i'], d['p'], d['s'], timestamp, exchange)
    async def insert_prev_day_data(self, prev_day_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare SQL queries
                lookup_query = 'SELECT ticker_id FROM tickers WHERE ticker_symbol = $1;'
                insert_query = '''
                INSERT INTO prev_day (ticker_id, c, h, l, o, v, vw)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (ticker_id) DO UPDATE 
                SET c = EXCLUDED.c,
                    h = EXCLUDED.h,
                    l = EXCLUDED.l,
                    o = EXCLUDED.o,
                    v = EXCLUDED.v,
                    vw = EXCLUDED.vw;
                '''
                
                # Loop through each prev_day_data entry
                for d in prev_day_data:
                    # Look up the ticker_id for the given ticker_symbol
                    ticker_id = await connection.fetchval(lookup_query, d['ticker_symbol'])
                    
                    # Insert data into the prev_day table
                    if ticker_id is not None:
                        await connection.execute(insert_query, ticker_id, d['c'], d['h'], d['l'], d['o'], d['v'], d['vw'])

    async def insert_min_data(self, min_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Prepare SQL queries
                lookup_query = 'SELECT ticker_id FROM tickers WHERE ticker_symbol = $1;'
                insert_query = '''
                INSERT INTO min (ticker_id, av, c, h, l, n, o, t, v, vw)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (ticker_id) DO UPDATE 
                SET av = EXCLUDED.av,
                    c = EXCLUDED.c,
                    h = EXCLUDED.h,
                    l = EXCLUDED.l,
                    n = EXCLUDED.n,
                    o = EXCLUDED.o,
                    t = EXCLUDED.t,
                    v = EXCLUDED.v,
                    vw = EXCLUDED.vw;
                '''
                
                # Loop through each min_data entry
                for d in min_data:
                    if d['t']:
                        timestamp = datetime.fromtimestamp(d['t'] / 1e9)
                        # Look up the ticker_id for the given ticker_symbol
                        ticker_id = await connection.fetchval(lookup_query, d['ticker_symbol'])
                        
                        # Insert data into the min table
                        if ticker_id is not None:
                            await connection.execute(
                                insert_query, ticker_id, d['av'], d['c'], d['h'], d['l'], d['n'], d['o'], timestamp, d['v'], d['vw']
                            )
    async def insert_options_main(self, data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                



                # The rest of the function remains the same
                # ... (except use `ticker_id` instead of `data['ticker']`)
                
                insert_query = """
                INSERT INTO options_main (
                    break_even_price, 
                    implied_volatility, 
                    open_interest, 
                    name, 
                    market_status, 
                    ticker,  -- Updated from `ticker`
                    type, 
                    contract_type, 
                    exercise_style, 
                    expiration_date, 
                    strike_price
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (ticker) DO UPDATE  -- Updated from `ticker`
                SET
                    break_even_price = EXCLUDED.break_even_price,
                    implied_volatility = EXCLUDED.implied_volatility,
                    open_interest = EXCLUDED.open_interest,
                    market_status = EXCLUDED.market_status
                """
                for d in data:
                    if d['expiration_date'] is not None:
                        expiration_date = datetime.strptime(d['expiration_date'], '%Y-%m-%d').date()
                        await connection.execute(
                            insert_query, 
                            d['break_even_price'], 
                            d['implied_volatility'], 
                            d['open_interest'], 
                            d['name'], 
                            d['market_status'], 
                            d['ticker'],  # Use th
                            d['type'], 
                            d['contract_type'], 
                            d['exercise_style'], 
                            expiration_date, 
                            d['strike_price']
                        )



    async def insert_options_session(self, data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                insert_query = """
                INSERT INTO options_session (
                    option_ticker, 
                    change, 
                    change_percent, 
                    early_trading_change, 
                    early_trading_change_percent, 
                    close, 
                    high, 
                    low, 
                    open, 
                    volume, 
                    previous_close
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (option_ticker) DO UPDATE  -- Updated from `ticker`
                SET
                    change = EXCLUDED.change,
                    change_percent = EXCLUDED.change_percent,
                    early_trading_change = EXCLUDED.early_trading_change,
                    early_trading_change_percent = EXCLUDED.early_trading_change_percent,
                    close = EXCLUDED.close,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    open = EXCLUDED.open,
                    volume = EXCLUDED.volume,
                    previous_close = EXCLUDED.previous_close;
                """
                for d in data:
                    session_data = d.get('session', {})
                    await connection.execute(
                        insert_query, 
                        d['ticker'],  # This assumes you've added the 'ticker' key to each dict in `data`
                        session_data.get('change', None),
                        session_data.get('change_percent', None),
                        session_data.get('early_trading_change', None),
                        session_data.get('early_trading_change_percent', None),
                        session_data.get('close', None),
                        session_data.get('high', None),
                        session_data.get('low', None),
                        session_data.get('open', None),
                        session_data.get('volume', None),
                        session_data.get('previous_close', None)
                    )
    async def insert_options_greeks(self, data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                insert_query = """
                INSERT INTO options_greeks (
                    delta, 
                    gamma, 
                    theta, 
                    vega,
                    option_ticker
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (option_ticker) DO UPDATE
                SET
                    delta = EXCLUDED.delta,
                    gamma = EXCLUDED.gamma,
                    theta = EXCLUDED.theta,
                    vega = EXCLUDED.vega;
                """
                for d in data:
                    if d['delta']:
                        await connection.execute(
                            insert_query, 
                            d['delta'], 
                            d['gamma'], 
                            d['theta'], 
                            d['vega'],
                            d['option_ticker']
                        )
    
    async def insert_options_last_trade(self, data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                insert_query = """
                INSERT INTO options_last_trade (
                    sip_timestamp,
                    conditions,
                    price,
                    size,
                    exchange,
                    option_ticker
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (option_ticker) DO UPDATE
                SET
                    sip_timestamp = EXCLUDED.sip_timestamp,
                    conditions = EXCLUDED.conditions,
                    price = EXCLUDED.price,
                    size = EXCLUDED.size,
                    exchange = EXCLUDED.exchange;
                """
                for d in data:
                    if 'last_trade' in d and d['last_trade'] is not None and d['last_trade'] is not {}:
                        if 'sip_timestamp' in d and d['last_trade']['sip_timestamp']:
                            timestamp = datetime.fromtimestamp(d['last_trade']['sip_timestamp'] / 1e9)
                        if 'exchange' in d and d['last_trade']['exchange']:
                            exchange = OPTIONS_EXCHANGES.get(d['last_trade']['exchange'])
                        if 'conditions' in d and d['last_trade']['conditions']:
                            conditions = [option_condition_dict.get(i) for i in d['last_trade']['conditions']] if d['last_trade']['conditions'] is not None else None
                            # Look up the ticker_id for the given ticker_symbol

                            
                            await connection.execute(
                                insert_query, 
                                timestamp, 
                                conditions[0], 
                                d['last_trade']['price'], 
                                d['last_trade']['size'], 
                                exchange,
                                d['ticker']
                            )


    async def insert_options_last_quote(self, data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # Lookup the ticker from the options_main table
                lookup_query = "SELECT ticker FROM options_main WHERE ticker = $1;"
                
                insert_query = """
                INSERT INTO options_last_quote (
                    ask, 
                    ask_size, 
                    bid, 
                    bid_size, 
                    midpoint, 
                    timeframe, 
                    option_ticker,
                    ask_exchange,
                    bid_exchange
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (option_ticker) DO UPDATE
                SET
                    ask = EXCLUDED.ask,
                    ask_size = EXCLUDED.ask_size,
                    bid = EXCLUDED.bid,
                    bid_size = EXCLUDED.bid_size,
                    midpoint = EXCLUDED.midpoint,
                    timeframe = EXCLUDED.timeframe,
                    ask_exchange = EXCLUDED.ask_exchange,
                    bid_exchange = EXCLUDED.bid_exchange;
                """
                for d in data:
                    option_ticker = d['option_ticker']
                    exists = await connection.fetchval('SELECT EXISTS(SELECT 1 FROM options_main WHERE ticker=$1)', option_ticker)
                    if exists:
                        ask_exchange = OPTIONS_EXCHANGES.get(d['ask_exchange'])
                        bid_exchange = OPTIONS_EXCHANGES.get(d['bid_exchange'])
                        await connection.execute(
                            insert_query,
                            
                            d['ask'],
                            d['ask_size'],
                            
                            d['bid'],
                            d['bid_size'],
                            
                            d['midpoint'],
                            d['timeframe'],
                            d['option_ticker'],
                            ask_exchange,
                            bid_exchange
                        )


    async def insert_options_underlying_asset(self, data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                insert_query = """
                INSERT INTO options_underlying_asset (
                    change_to_break_even,
                    price,
                    ticker,
                    timeframe
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (option_ticker) DO UPDATE
                SET
                    change_to_break_even = EXCLUDED.change_to_break_even,
                    price = EXCLUDED.price,
                    ticker = EXCLUDED.ticker,
                    timeframe = EXCLUDED.timeframe;
                """
                for d in data:
                    await connection.execute(
                        insert_query, 
                        d['change_to_break_even'] if 'change_to_break_even' in d else 0,
                        d['price'] if 'price' in d else d['value'] if 'value' in d else None,
                        d['ticker'],
                        d['timeframe'],
                    )




    async def insert_dark_pool_activity(self, ticker, time, size, price, notional_value, message_id):
        try:
            naive_time = time.astimezone(pytz.utc).replace(tzinfo=None)
            await self.pool.execute("""
                INSERT INTO dark_pools (ticker, time, size, price, notional_value, message_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (message_id)
                DO NOTHING;
            """, ticker, naive_time, size, price, notional_value, message_id)
        except UniqueViolationError:
            print(f'Skipping for {ticker} - already exists in database.')

    async def insert_unusual_option_activity(self, ticker, sentiment, strike, contract_type, expiration_date,
                                                volume, open_interest, size, premium, trade_side, consolidation_type, trade_type, reference_price,is_unusual,
                                                is_golden_sweep, is_opening_position, timestamp, sector, message_id):
        await self.pool.execute("""
            INSERT INTO uoa (ticker, sentiment, strike, contract_type, expiration_date,
                                                volume, open_interest, size, premium, trade_side, consolidation_type, trade_type, reference_price, is_unusual,
                                                is_golden_sweep, is_opening_position, sector)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
        """, ticker, sentiment, strike, contract_type, expiration_date,
            volume, open_interest, size, premium, trade_side, consolidation_type, trade_type, reference_price, is_unusual,
            is_golden_sweep, is_opening_position, timestamp,sector, message_id)




    async def insert_flow_activity(self, message_id, ticker, sentiment, strike, contract_type, expiry, side, volume, open_interest, vol_oi, iv, dtx, premium,reference_price, timestamp_field, flow_type, color):
        try:
        # Convert the string to a Pandas Timestamp
            expiry_timestamp = pd.to_datetime(expiry)

            await self.pool.execute("""
                INSERT INTO opening_flow (message_id,ticker, sentiment, strike, contract_type, expiry, side, volume, open_interest, vol_oi, iv, dtx, premium, reference_price, timestamp, flow_type, color)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                ON CONFLICT (message_id)
                DO NOTHING;
            """, message_id, ticker, sentiment, strike, contract_type, expiry_timestamp, side, volume, open_interest, vol_oi, iv, dtx, premium,reference_price, timestamp_field, flow_type, color)
        except UniqueViolationError:
            print('ERROR - FLOW ACTIVITY')

    async def insert_stock_equity_trade_batch(self, batch):
        async with self.pool.acquire() as connection:
            query = '''
            INSERT INTO stock_trades (stock_symbol, exchange, price, size, conditions, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            '''
            await connection.executemany(query, [(item['stock_symbol'], item['exchange'], item['price'], item['size'], item['conditions'], item['timestamp']) for item in batch])


    async def insert_stock_equity_agg_batch(self, batch):
        async with self.pool.acquire() as connection:
            query = '''
            INSERT INTO stockaggs (stock_symbol, close, high, open, low, volume, official_open, total_volume, vwap, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            '''
            await connection.executemany(query, [(item['stock_symbol'], item['close'], item['high'], item['open'], item['low'], item['volume'], item['official_open'], item['total_volume'], item['vwap'], item['timestamp']) for item in batch])

    async def insert_ticker_aggregates(self, batch):
        async with self.pool.acquire() as connection:
            query = '''
            INSERT INTO stockaggs (open, high, low, close, timestamp, volume)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            '''
            await connection.executemany(query, [(item['ticker'], item['open'], item['high'], item['low'], item['close'], item['timestamp'], item['volume']) for item in batch])

    async def insert_equity_option_agg_batch(self, batch):
        async with self.pool.acquire() as connection:
            query = '''
            INSERT INTO equity_option_agg (expiry, call_put, strike, underlying_symbol, option_symbol, total_volume, volume, vwap, official_open, last_price, open, price_diff, moneyness, price_vwap_diff, price_percent_change, volume_percent_total, volume_to_price, timestamp, hour_of_day)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            '''
            await connection.executemany(query, [(item['expiry'], item['call_put'], item['strike'], item['underlying_symbol'], item['option_symbol'], item['total_volume'], item['volume'], item['vwap'], item['official_open'], item['last_price'], item['open'], item['price_diff'], item['moneyness'], item['price_vwap_diff'], item['price_percent_change'], item['volume_percent_total'], item['volume_to_price'], item['timestamp'], item['hour_of_day']) for item in batch])
    async def insert_equity_quote_batch(self, batch):
        async with self.pool.acquire() as connection:
            query = '''
            INSERT INTO equity_quote (quote_symbol, ask, bid, ask_size, bid_size, ask_exchange, bid_exchange, timestamp, hour_of_day)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            '''
            await connection.executemany(query, [(item['quote_symbol'], item['ask'], item['bid'], item['ask_size'], item['bid_size'], item['ask_exchange'], item['bid_exchange'], item['timestamp'], item['hour_of_day']) for item in batch])

    async def insert_equity_option_trade_batch(self, batch):
        async with self.pool.acquire() as connection:
            query = '''
            INSERT INTO equity_option_trade (expiry, option_symbol, call_put, strike, underlying_symbol, price, price_change, size, volume_change, conditions, exchange, price_to_strike, hour_of_day, weekday, timestamp, dollar_cost)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            '''
            await connection.executemany(query, [(item['expiry'], item['option_symbol'], item['call_put'], item['strike'], item['underlying_symbol'], item['price'], item['price_change'], item['size'], item['volume_change'], item['conditions'], item['exchange'], item['price_to_strike'], item['hour_of_day'], item['weekday'], item['timestamp'], item['dollar_cost']) for item in batch])
    async def close(self):
        if self.conn:
            await self.conn.close()

