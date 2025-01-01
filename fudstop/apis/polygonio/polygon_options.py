from tabulate import tabulate
from dotenv import load_dotenv
import httpx
load_dotenv()
from fudstop.apis.helpers import convert_to_eastern_time
from fudstop.apis.polygonio.mapping import OPTIONS_EXCHANGES
from .models.technicals import RSI
from aiohttp.client_exceptions import ClientConnectionError, ClientConnectorError, ClientError, ClientOSError,ContentTypeError
import os
from .polygon_helpers import get_human_readable_string
import sys
import pandas as pd
import re
from fudstop.all_helpers import chunk_string
from pathlib import Path
import aiohttp
import asyncpg
import logging
import asyncio
from fudstop._markets.list_sets.dicts import option_conditions
from asyncpg.exceptions import UniqueViolationError
from asyncpg import create_pool
from asyncio import Lock
from urllib.parse import urlencode
from datetime import datetime, timedelta
from .models.option_models.option_snapshot import WorkingUniversal


import numpy as np
from .models.option_models.universal_snapshot import UniversalOptionSnapshot,UniversalOptionSnapshot2, SpxSnapshot
from .polygon_helpers import flatten_nested_dict, flatten_dict
lock = Lock()

sema = asyncio.Semaphore(4)
# Function to map Pandas dtypes to PostgreSQL types
def dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'REAL'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    elif pd.api.types.is_string_dtype(dtype):
        return 'TEXT'
    else:
        return 'TEXT'  # Default type
import pytz
from datetime import timezone
# Example conversion function
# Example conversion function
# Example conversion function for Unix nanosecond timestamps
def convert_ns_to_est(ns_timestamp):
    # Convert nanoseconds to seconds
    timestamp_in_seconds = ns_timestamp / 1e9
    
    # Convert to a datetime object in UTC
    utc_time = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)
    
    # Convert to EST
    est_time = utc_time.astimezone(pytz.timezone('US/Eastern'))
    
    # Remove timezone info
    est_time = est_time.replace(tzinfo=None)
    
    # Format the datetime object as a string
    est_time_str = est_time.strftime("%Y-%m-%d %H:%M:%S")
    
    return est_time_str


class PolygonOptions:
    def __init__(self,user:str='chuck', database:str='market_data', host:str='localhost', port:int=5432, password:str='fud'):
        self.conn = None
        self.pool = None
        self.host=host
        self.port=port
        self.password=password
        self.user=user
        self.database=database


        self.session = None
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.one_year_from_now = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        self.one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
     
        self.connection_string = "postgresql://chuck:fud@localhost:5432/market_data"
        self._45_days_from_now = (datetime.now() + timedelta(days=45)).strftime('%Y-%m-%d')
        self._90_days_drom_now = (datetime.now() + timedelta(days=90)).strftime('%Y-%m-%d')

        self.db_config = {
            "host": host, # Default to this IP if 'DB_HOST' not found in environment variables
            "port": port, # Default to 5432 if 'DB_PORT' not found
            "user": user, # Default to 'postgres' if 'DB_USER' not found
            "password": password, # Use the password from environment variable or default
            "database": database # Database name for the new jawless database
        }
# Asynchronous method to create an aiohttp session
    async def create_http_session(self):
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession()
        return self.http_session
    # Asynchronous method to get the existing aiohttp session or create a new one
    async def get_http_session(self):
        return self.http_session if self.http_session and not self.http_session.closed else await self.create_http_session()

    # Asynchronous method to close an aiohttp session
    async def close_http_session(self):
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            self.http_session = None

    async def fetch_page(self, session, url):
        if 'apiKey' not in url:
            url = f"{url}?apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
        try:
            response = await session.get(url)
            return response.json()
        except Exception as e:
            print(f"ERROR: {e}")

    async def paginate_concurrent(self, url, as_dataframe=False, concurrency=250, filter:bool=False):
        all_results = []
        pages_to_fetch = [url]

        async with httpx.AsyncClient() as session:
            while pages_to_fetch:
                tasks = []
                current_urls = pages_to_fetch[:concurrency]
                pages_to_fetch = pages_to_fetch[concurrency:]  # Update remaining pages

                for next_url in current_urls:
                    tasks.append(self.fetch_page(session, next_url))

                results = await asyncio.gather(*tasks)
                for data in results:
                    if data is not None and "results" in data:
                        all_results.extend(data["results"])
                        next_url = data.get("next_url")
                        if next_url:
                            next_url += f'&{urlencode({"apiKey": os.environ.get("YOUR_POLYGON_KEY")})}'
                            pages_to_fetch.append(next_url)

        if as_dataframe:
            import pandas as pd
            return pd.DataFrame(all_results)
            
        return all_results
    def sanitize_value(self, value, col_type):
        """Sanitize and format the value for SQL query."""
        if col_type == 'str':
            # For strings, add single quotes
            return f"'{value}'"
        elif col_type == 'date':
            # For dates, format as 'YYYY-MM-DD'
            if isinstance(value, str):
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                    return f"'{value}'"
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
            elif isinstance(value, datetime):
                return f"'{value.strftime('%Y-%m-%d')}'"
        else:
            # For other types, use as is
            return str(value)

    async def connect(self):
        """Initialize the connection pool if it doesn't already exist."""
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                user=self.user,
                port=self.port,
                database=self.database,
                password=self.password,
                min_size=1,  # Minimum number of connections in the pool
                max_size=10  # Adjust based on workload and DB limits
            )
        return self.pool

    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def fetch_new(self, query, *args):
        """Fetch data from the database."""
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def execute(self, query, *args):
        """Execute a query in the database."""
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)
    
    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
    async def update_options(self, ticker):
        all_options = await self.find_symbols(ticker)
        df = all_options.df
        print(df)
        await self.batch_insert_dataframe(df, table_name='opts', unique_columns='option_symbol')



    
    async def filter_options(self,select_columns:str=None, **kwargs):
        """
        Filters the options table based on provided keyword arguments.
        Usage example:
            await filter_options(strike_price_min=100, strike_price_max=200, call_put='call',
                                 expire_date='2023-01-01', delta_min=0.1, delta_max=0.5)
        """
        # Start with the base query
        query = f"SELECT * FROM opts WHERE "
        params = []
        param_index = 1

        # Mapping kwargs to database columns and expected types, including range filters
        column_types = {
            'ticker': ('ticker', 'string'),
            'strike': ('strike', 'float'),
            'strike_min': ('strike', 'float'),
            'strike_max': ('strike', 'float'),
            'expiry': ('expiry', 'date'),
            'expiry_min': ('expiry', 'date'),
            'expiry_max': ('expiry', 'date'),
            'open': ('open', 'float'),
            'open_min': ('open', 'float'),
            'open_max': ('open', 'float'),
            'high': ('high', 'float'),
            'high_min': ('high', 'float'),
            'high_max': ('high', 'float'),
            'low': ('low', 'float'),
            'low_min': ('low', 'float'),
            'low_max': ('low', 'float'),
            'oi': ('oi', 'float'),
            'oi_min': ('oi', 'float'),
            'oi_max': ('oi', 'float'),
            'vol': ('vol', 'float'),
            'vol_min': ('vol', 'float'),
            'vol_max': ('vol', 'float'),
            'delta': ('delta', 'float'),
            'delta_min': ('delta', 'float'),
            'delta_max': ('delta', 'float'),
            'vega': ('vega', 'float'),
            'vega_min': ('vega', 'float'),
            'vega_max': ('vega', 'float'),
            'iv': ('iv', 'float'),
            'iv_min': ('iv', 'float'),
            'iv_max': ('iv', 'float'),
            'dte': ('dte', 'string'),
            'dte_min': ('dte', 'string'),
            'dte_max': ('dte', 'string'),
            'gamma': ('gamma', 'float'),
            'gamma_min': ('gamma', 'float'),
            'gamma_max': ('gamma', 'float'),
            'theta': ('theta', 'float'),
            'theta_min': ('theta', 'float'),
            'theta_max': ('theta', 'float'),
            'sensitivity': ('sensitivity', 'float'),
            'sensitivity_max': ('sensitivity', 'float'),
            'sensitivity_min': ('sensitivity', 'float'),
            'bid': ('bid', 'float'),
            'bid_min': ('bid', 'float'),
            'bid_max': ('bid', 'float'),
            'ask': ('ask', 'float'),
            'ask_min': ('ask', 'float'),
            'ask_max': ('ask', 'float'),
            'close': ('close', 'float'),
            'close_min': ('close', 'float'),
            'close_max': ('close', 'float'),
            'cp': ('cp', 'string'),
            'time_value': ('time_value', 'float'),
            'time_value_min': ('time_value', 'float'),
            'time_value_max': ('time_value', 'float'),
            'moneyness': ('moneyness', 'string'),
            'exercise_style': ('exercise_style', 'string'),
            'option_symbol': ('option_symbol', 'string'),
            'theta_decay_rate': ('theta_decay_rate', 'float'),
            'theta_decay_rate_min': ('theta_decay_rate', 'float'),
            'theta_decay_rate_max': ('theta_decay_rate', 'float'),
            'delta_theta_ratio': ('delta_theta_ratio', 'float'),
            'delta_theta_ratio_min': ('delta_theta_ratio', 'float'),
            'delta_theta_ratio_max': ('delta_theta_ratio', 'float'),
            'gamma_risk': ('gamma_risk', 'float'),
            'gamma_risk_min': ('gamma_risk', 'float'),
            'gamma_risk_max': ('gamma_risk', 'float'),
            'vega_impact': ('vega_impact', 'float'),
            'vega_impact_min': ('vega_impact', 'float'),
            'vega_impact_max': ('vega_impact', 'float'),
            'intrinsic_value_min': ('intrinsic_value', 'float'),
            'intrinsic_value_max': ('intrinsic_value', 'float'),
            'intrinsic_value': ('intrinsic_value', 'float'),
            'extrinsic_value': ('extrinsic_value', 'float'),
            'extrinsic_value_min': ('extrinsic_value', 'float'),
            'extrinsic_value_max': ('extrinsic_value', 'float'),
            'leverage_ratio': ('leverage_ratio', 'float'),
            'leverage_ratio_min': ('leverage_ratio', 'float'),
            'leverage_ratio_max': ('leverage_ratio', 'float'),
            'vwap': ('vwap', 'float'),
            'vwap_min': ('vwap', 'float'),
            'vwap_max': ('vwap', 'float'),
            'price': ('price', 'float'),
            'price_min': ('price', 'float'),
            'price_max': ('price', 'float'),
            'trade_size': ('trade_size', 'float'),
            'trade_size_min': ('trade_size', 'float'),
            'trade_size_max': ('trade_size', 'float'),
            'spread': ('spread', 'float'),
            'spread_min': ('spread', 'float'),
            'spread_max': ('spread', 'float'),
            'spread_pct': ('spread_pct', 'float'),
            'spread_pct_min': ('spread_pct', 'float'),
            'spread_pct_max': ('spread_pct', 'float'),
            'bid_size': ('bid_size', 'float'),
            'bid_size_min': ('bid_size', 'float'),
            'bid_size_max': ('bid_size', 'float'),
            'ask_size': ('ask_size', 'float'),
            'ask_size_min': ('ask_size', 'float'),
            'ask_size_max': ('ask_size', 'float'),
            'mid': ('mid', 'float'),
            'mid_min': ('mid', 'float'),
            'mid_max': ('mid', 'float'),
            'change_to_breakeven': ('change_to_breakeven', 'float'),
            'change_to_breakeven_min': ('change_to_breakeven', 'float'),
            'change_to_breakeven_max': ('change_to_breakeven', 'float'),
            'underlying_price': ('underlying_price', 'float'),
            'underlying_price_min': ('underlying_price', 'float'),
            'underlying_price_max': ('underlying_price', 'float'),
            'return_on_risk': ('return_on_risk', 'float'),
            'return_on_risk_min': ('return_on_risk', 'float'),
            'return_on_risk_max': ('return_on_risk', 'float'),
            'velocity': ('velocity', 'float'),
            'velocity_min': ('velocity', 'float'),
            'velocity_max': ('velocity', 'float'),
            'greeks_balance': ('greeks_balance', 'float'),
            'greeks_balance_min': ('greeks_balance', 'float'),
            'greeks_balance_max': ('greeks_balance', 'float'),
            'opp': ('opp', 'float'),
            'opp_min': ('opp', 'float'),
            'opp_max': ('opp', 'float'),
            'liquidity_score': ('liquidity_score', 'float'),
            'liquidity_score_min': ('liquidity_score', 'float'),
            'liquidity_score_max': ('liquidity_score', 'float')
            }

        
     
        # Dynamically build query based on kwargs
        query = f"SELECT * FROM opts WHERE oi > 0" if select_columns is None else f"SELECT ticker, strike, cp, expiry, {select_columns} FROM opts WHERE oi > 0"

        

        # Dynamically build query based on kwargs
        for key, value in kwargs.items():
            if key in column_types and value is not None:
                column, col_type = column_types[key]

                # Sanitize and format value for SQL query
                sanitized_value = self.sanitize_value(value, col_type)

                if 'min' in key:
                    query += f" AND {column} >= {sanitized_value}"
                elif 'max' in key:
                    query += f" AND {column} <= {sanitized_value}"
                else:
                    query += f" AND {column} = {sanitized_value}"
                print(query)
        

        try:
            async with self.pool.acquire() as conn:
                # Execute the query
                return await conn.fetch(query)
        except Exception as e:
            print(f"Error during query: {e}")
            return []
    async def fetch_filtered_records(self, **kwargs):
        # Start with the base SELECT statement
        select_sql = "SELECT ticker, strike, expiry, cp FROM opts"

        # Define which attributes can have min/max filters
        numeric_attrs = ['strike', 'dte', 'time_value', 'liquidity_score', 'theta', 
                         'theta_decay_rate', 'delta', 'delta_theta_ratio', 'gamma', 
                         'gamma_risk', 'vega', 'vega_impact', 'timestamp', 'oi', 'open', 
                         'high', 'low', 'close', 'intrinstic_value', 'extrinsic_value', 
                         'leverage_ratio', 'vwap', 'conditions', 'price', 'trade_size', 
                         'ask', 'bid', 'spread', 'spread_pct', 'iv', 'bid_size', 
                         'ask_size', 'vol', 'mid', 'change_to_breakeven', 
                         'underlying_price', 'return_on_risk', 'velocity', 'sensitivity', 
                         'greeks_balance', 'opp']

        # Add WHERE clauses based on kwargs
        conditions = []
        values = []
        for column, value in kwargs.items():
            print(kwargs)
            if column in numeric_attrs:
                if isinstance(value, dict):
                    # Handle min/max conditions for numeric attributes
                    for operation, op_value in value.items():
                        if operation == 'min':
                            conditions.append(f"{column} >= ${len(conditions) + 1}")
                            values.append(op_value)
                        elif operation == 'max':
                            conditions.append(f"{column} <= ${len(conditions) + 1}")
                            values.append(op_value)
                else:
                    # Handle exact match for numeric attributes
                    conditions.append(f"{column} = ${len(conditions) + 1}")
                    values.append(value)
            else:
                # Handle exact match for non-numeric attributes
                conditions.append(f"{column} = ${len(conditions) + 1}")
                values.append(value)



        if conditions:
            select_sql += " WHERE " + " AND ".join(conditions)




        try:
            async with self.pool.acquire() as conn:
                # Fetch records with the constructed query and values
                records = await conn.fetch(select_sql, *values)
                return records
        except asyncpg.PostgresError as e:
            print(f"Database error: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

# Example usage

    async def fetch_records(self):
        await self.connect()
        select_sql = "SELECT * FROM opts"  # Modify this query based on your specific needs
        async with self.pool.acquire() as conn:
            # Fetch records
            records = await conn.fetch(select_sql)
            return records
    async def create_table(self, df, table_name, unique_column, create_history=False):
     
        print("Connected to the database.")
        dtype_mapping = {
                'int64': 'INTEGER',
                'float64': 'FLOAT',
                'object': 'TEXT',
                'bool': 'BOOLEAN',
                'datetime64': 'TIMESTAMP',
                'datetime64[ns]': 'timestamp',
                'datetime64[ms]': 'timestamp',
                'datetime64[ns, US/Eastern]': 'TIMESTAMP WITH TIME ZONE',
                'string': 'TEXT',
                'int32': 'INTEGER',
                'float32': 'FLOAT',
                'datetime64[us]': 'timestamp',
                'timedelta[ns]': 'INTERVAL',
                'category': 'TEXT',
                'int16': 'SMALLINT',
                'int8': 'SMALLINT',
                'uint8': 'SMALLINT',
                'uint16': 'INTEGER',
                'uint32': 'BIGINT',
                'uint64': 'NUMERIC',
                'complex64': 'COMPLEX',
                'complex128': 'COMPLEX',
                'bytearray': 'BLOB',
                'bytes': 'BLOB',
                'memoryview': 'BLOB',
                'list': 'INTEGER[]'  # New mapping for array of integers
            }
            
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
                # Ensure unique_column is handled correctly, either as a list or a comma-separated string
                if isinstance(unique_column, str):
                    unique_column = [col.strip() for col in unique_column.split(",")]
                
                # Join the unique_column list with commas to form the unique constraint string
                unique_constraint = f'UNIQUE ({", ".join(unique_column)})' if unique_column else ''
                
                create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}' for col, dtype in zip(df.columns, df.dtypes))},
                    "insertion_timestamp" TIMESTAMP,
                    {unique_constraint}
                )
                """
        
                print(f"Creating table with query: {create_query}")
                try:
                    await connection.execute(create_query)
                    print(f"Table {table_name} created successfully.")
                except asyncpg.UniqueViolationError as e:
                    print(f"Unique violation error: {e}")
            else:
                print(f"Table {table_name} already exists.")
            
            # Optional history table creation
            if create_history:
                # Create the history table
                history_create_query = f"""
                CREATE TABLE IF NOT EXISTS {history_table_name} (
                    operation CHAR(1) NOT NULL,
                    changed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                    {', '.join(f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}' for col, dtype in zip(df.columns, df.dtypes))}
                );
                """
                print(f"Creating history table with query: {history_create_query}")
                await connection.execute(history_create_query)
                
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

                print(f"History table {history_table_name} and trigger created successfully.")
            
            # Alter existing table to add any missing columns
            for col, dtype in zip(df.columns, df.dtypes):
                alter_query = f"""
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE {table_name} ADD COLUMN "{col}" {dtype_mapping.get(str(dtype), "TEXT")};
                    EXCEPTION
                        WHEN duplicate_column THEN
                        NULL;
                    END;
                END $$;
                """
                await connection.execute(alter_query)
    async def fetch(self, query):
        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query)
                return records
        except Exception as e:
            print(e)
    async def get_trades(self, symbol, client):
        try:
            response = await client.get(
                f"https://api.polygon.io/v3/trades/{symbol}?limit=50000&apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
            )
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])

                if not results:
                    return None

                # Parse the symbol to extract ticker, expiry, call_put, and strike
                pattern = r'^O:(?P<ticker>[A-Z]+)(?P<expiry>\d{6})(?P<call_put>[CP])(?P<strike>\d{8})$'
                match = re.match(pattern, symbol)
                if match:
                    ticker = match.group('ticker')
                    expiry_str = match.group('expiry')
                    call_put = match.group('call_put')
                    strike_str = match.group('strike')

                    # Convert expiry to date format
                    expiry = datetime.strptime(expiry_str, '%y%m%d').date()

                    # Convert strike price to float
                    strike = int(strike_str) / 1000.0
                else:
                    # If the symbol doesn't match the expected pattern, assign default values or skip
                    print(f"Symbol {symbol} didn't match the expected pattern.")
                    ticker = ''
                    expiry = None
                    call_put = ''
                    strike = 0.0

                # Map conditions to descriptions
                conditions = [
                    ', '.join(option_conditions.get(condition, 'Unknown Condition') for condition in trade.get('conditions', []))
                    for trade in results
                ]

                # Corrected exchange mapping
                exchanges = [
                    OPTIONS_EXCHANGES.get(trade.get('exchange'), 'Unknown Exchange')
                    for trade in results
                ]

                ids = [trade.get('id') for trade in results]
                prices = [trade.get('price') for trade in results]
                sequence_numbers = [trade.get('sequence_number') for trade in results]
                sip_timestamps = [convert_to_eastern_time(trade.get('sip_timestamp')) for trade in results]
                sizes = [trade.get('size') for trade in results]

                # Compute dollar cost per trade
                dollar_costs = [price * size for price, size in zip(prices, sizes)]

                data_dict = {
                    'ticker': [ticker]*len(results),             # Underlying ticker
                    'strike': [strike]*len(results),             # Strike price
                    'call_put': [call_put]*len(results),         # 'C' or 'P'
                    'expiry': [expiry]*len(results),             # Expiry date
                    'option_symbol': [symbol]*len(results),      # Option symbol
                    'conditions': conditions,
                    'price': prices,
                    'size': sizes,
                    'dollar_cost': dollar_costs,                 # Dollar cost per trade
                    'timestamp': sip_timestamps,
                    'sequence_number': sequence_numbers,
                    'id': ids,
                    'exchange': exchanges
                }

                df = pd.DataFrame(data_dict)

                # Compute average price and average size for this symbol
                average_price = df['price'].mean()
                average_size = df['size'].mean()
                highest_price = df['price'].max()
                lowest_price = df['price'].min()
                highest_size = df['size'].max()
            

                # Add the averages as new columns with the same value for all rows
                df['average_price'] = average_price
                df['average_size'] = average_size
                df['highest_price'] = highest_price
                df['lowest_price'] = lowest_price
                df['highest_size'] = highest_size
                await self.batch_upsert_dataframe(df, table_name='option_trades', unique_columns=['option_symbol', 'timestamp', 'sequence_number', 'exchange', 'conditions', 'size', 'price', 'dollar_cost'])
                return df
            else:
                print(f"Failed to retrieve data for {symbol}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(e)


    async def get_price(self, ticker):
        try:
            if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX']:
                ticker = f"I:{ticker}"
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            print(url)
            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                if r.status_code == 200:
                    r = r.json()
                    results = r['results'] if 'results' in r else None
                    if results is not None:
                        session = [i.get('session') for i in results]
                        price = [i.get('close') for i in session]
                        return price[0]
        except Exception as e:
            print(f"{ticker} ... {e}")


    async def get_trades_for_symbols(self, symbols):
        async with httpx.AsyncClient() as client:
            tasks = [self.get_trades(symbol, client) for symbol in symbols]
            results = await asyncio.gather(*tasks)
            # Filter out None results and concatenate dataframes
            dataframes = [df for df in results if df is not None]
            if dataframes:
                all_data = pd.concat(dataframes, ignore_index=True)
                return all_data
            else:
                return pd.DataFrame()  # Return empty DataFrame if no data
    async def get_symbols(self, ticker):
        try:
            # Fetch the current price of the ticker
            price = await self.get_price(ticker)
            print(f"Price for {ticker}: {price}")

            # Fetch the option chain within the specified strike price and expiration date range
            options_df = await self.get_option_chain_all(
                underlying_asset=ticker,
                expiration_date_gte=self.today,
                expiration_date_lte=self.fifteen_days_from_now
            )
            print(f"Number of options found for {ticker}: {len(options_df.df)}")

            # Extract the option symbols
            symbols = options_df.option_symbol
            return symbols
        except Exception as e:
            print(e)

    async def fetch_option_aggs(self, option_symbol: str, start_date: str = "2021-01-01", timespan:str='day'):
        """
        Fetches the historical data for an option symbol from the specified start date to the current date.
        
        Args:
        option_symbol (str): The option symbol to get data for.
        start_date (str): The start date in 'YYYY-MM-DD' format from where to begin fetching the data.
        
        Returns:
        pd.DataFrame: A DataFrame containing the open, close, high, low, and other details for each day.
        """
        end_date = datetime.today().strftime('%Y-%m-%d')
        url_template = (
            "https://api.polygon.io/v2/aggs/ticker/{}/range/1/{}/{}/{}"
            "?adjusted=true&sort=asc&limit=50000&apiKey={}"
        )
        
        url = url_template.format(option_symbol, timespan, start_date, end_date, self.api_key)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    df = pd.DataFrame(data['results'])
                    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
                    df = df.rename(columns={
                        'o': 'open',
                        'c': 'close',
                        'h': 'high',
                        'l': 'low',
                        'v': 'volume'
                    })
                    df = df[['date', 'open', 'close', 'high', 'low', 'volume']]
                    return df
                else:
                    print(f"No data available for {option_symbol}.")
                    return pd.DataFrame()  # Return empty DataFrame if no results
            else:
                print(f"Failed to fetch data: {response.status_code}")
                return pd.DataFrame()  # Return empty DataFrame on failure
    async def find_lowest_highest_price_df(self, ticker: str, strike:int, expiry:str, call_put:str) -> pd.DataFrame:
        """
        Fetches the historical data for an option symbol and finds the lowest and highest prices 
        along with the dates they occurred, returning the result as a DataFrame.

        Args:
        option_symbol (str): The option symbol to get data for.
        start_date (str): The start date in 'YYYY-MM-DD' format from where to begin fetching the data.

        Returns:
        pd.DataFrame: A DataFrame containing the dates and prices for the lowest and highest points.
        """
        # Fetch the historical data
        await self.connect()
        query = f"""SELECT option_symbol from master_all_two where ticker = '{ticker}' and strike = {strike} and call_put = '{call_put}' and expiry = '{expiry}'"""
        results = await self.fetch(query)
        df = pd.DataFrame(results, columns=['option_symbol'])
        option_symbol = df['option_symbol'].to_list()[0]
        df = await self.fetch_option_aggs(option_symbol)
        
        if df.empty:
            return pd.DataFrame({
                "type": ["lowest", "highest"],
                "date": ["N/A", "N/A"],
                "price": [float('inf'), float('-inf')]
            })
        
        # Find the lowest price and the corresponding date
        min_row = df.loc[df['low'].idxmin()]
        lowest_price = min_row['low']
        lowest_date = min_row['date'].strftime('%Y-%m-%d')
        
        # Find the highest price and the corresponding date
        max_row = df.loc[df['high'].idxmax()]
        highest_price = max_row['high']
        highest_date = max_row['date'].strftime('%Y-%m-%d')
        
        # Create a DataFrame with the results
        result_df = pd.DataFrame({
            "type": ["lowest", "highest"],
            "date": [lowest_date, highest_date],
            "price": [lowest_price, highest_price]
        })
        
        return result_df
    async def rsi(self, ticker:str, timespan:str, limit:str='1000', window:int=14, date_from:str=None, date_to:str=None, session=None):
        """
        Arguments:

        >>> ticker

        >>> AVAILABLE TIMESPANS:

        minute
        hour
        day
        week
        month
        quarter
        year

        >>> date_from (optional) 
        >>> date_to (optional)
        >>> window: the RSI window (default 14)
        >>> limit: the number of N timespans to survey
        
        """

        if date_from is None:
            date_from = self.eight_days_ago

        if date_to is None:
            date_to = self.today


        endpoint = f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan={timespan}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&window={window}&apiKey={self.api_key}"
 

        try:
            async with self.session.get(endpoint) as resp:
                datas = await resp.json()
                if datas is not None:

                    

            
                    return RSI(datas, ticker)
        except (ClientConnectorError, ClientOSError, ContentTypeError):
            print(f"ERROR - {ticker}")
    async def rsi_snapshot(self, ticker:str):
        try:
            components = get_human_readable_string(ticker)
            symbol = components.get('underlying_symbol')
            strike = components.get('strike_price')
            expiry = components.get('expiry_date')
            call_put = components.get('call_put')
            call_put = str(call_put).lower()
            timespans = ['minute', 'hour', 'day', 'week', 'month']
            all_data_dicts=[]
            for timespan in timespans:
                rsi =await self.rsi(ticker, timespan, limit='1')
        

                # Check if rsi or rsi.rsi_value[0] is None
                # Check if rsi is not None and that rsi.rsi_value itself is not None and has at least one element
                if rsi is not None and rsi.rsi_value is not None and len(rsi.rsi_value) > 0:
                    data_dict = { 
                        'timespan': timespan,
                        'option_symbol': ticker,
                        'ticker': symbol,
                        'strike': strike,
                        'expiry': expiry,
                        'call_put': call_put,
                        'rsi': rsi.rsi_value[0]
                    }
                else:
                    # Handle the None case appropriately
                    # For example, you can set a default value or perform a different action
                    data_dict = { 
                        'timespan': timespan,
                        'option_symbol': ticker,
                        'ticker': symbol,
                        'strike': strike,
                        'expiry': expiry,
                        'call_put': call_put,
                        'rsi': 0  # Replace 'default_value' with an appropriate fallback
                    }
                all_data_dicts.append(data_dict)
            df = pd.DataFrame(all_data_dicts)
            
            return df
        except Exception as e:
            print(e)

    async def option_aggregates(self, ticker:str, timespan:str='second', date_from:str='2019-09-17', date_to:str=None, limit=50000, 
    as_dataframe:bool=False):
        """Gets all aggregates for a ticker, or option symbol
        
        Arguments:

        >>> ticker: the ticker to query

        >>> timespan:

            -second
            -minute
            -hour
            -day
            -week
            -month
            -quarter
            -year

        >>> date_from: the date to start in YYYY-MM-DD format (default 2019-09-17)

        >>> date_to: the date to end on in YYYY-MM-DD format (default today)


        >>> limit: the number of results to return. (default 50000)


        >>> as_dataframe: whether to return as a dataframe or not (default false)

        """

        if date_to == None:
            date_to = self.today
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}/{date_from}/{date_to}?adjusted=true&sort=desc&limit=50000&apiKey={self.api_key}"
        
        session = await self.fetch_page()

       

        results = await self.paginate_concurrent(url, as_dataframe=False, concurrency=50)
        if as_dataframe == True:
            return pd.DataFrame(results)
        return results
            



    async def batch_insert_dataframe(self, df, table_name, unique_columns, batch_size=250):
        try:
            async with lock:
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)
                try:
                    # Copy the DataFrame to avoid modifying the original
                    df = df.copy()

                    # Add the insertion_timestamp column with current datetime
                    df['insertion_timestamp'] = pd.to_datetime([datetime.now() for _ in range(len(df))])

                    # Convert DataFrame to a list of dictionaries
                    records = df.to_dict(orient='records')

                    async with self.pool.acquire() as connection:
                        column_types = await connection.fetch(
                            f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
                        )
                        type_mapping = {col['column_name']: col['data_type'] for col in column_types}

                        async with connection.transaction():
                            insert_query = f"""
                            INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in df.columns)}) 
                            VALUES ({', '.join('$' + str(i) for i in range(1, len(df.columns) + 1))})
                            ON CONFLICT ({unique_columns})
                            DO UPDATE SET {', '.join(f'"{col}" = excluded."{col}"' for col in df.columns)}
                            """
                            batch_data = []
                            for record in records:
                                new_record = []
                                for col, val in record.items():
                                    pg_type = type_mapping.get(col)

                                    if pd.isna(val):
                                        new_record.append(None)
                                    elif pg_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                                        if isinstance(val, np.datetime64):
                                            # Convert numpy datetime64 to Python datetime, ensure UTC and remove tzinfo if needed
                                            new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))
                                        elif isinstance(val, datetime):
                                            # Directly use the Python datetime object
                                            new_record.append(val)
                                    elif pg_type in ['double precision', 'real'] and not isinstance(val, str):
                                        new_record.append(float(val))
                                    elif isinstance(val, np.int64):
                                        new_record.append(int(val))
                                    elif pg_type == 'integer' and not isinstance(val, int):
                                        new_record.append(int(val))
                                    else:
                                        new_record.append(val)

                                batch_data.append(tuple(new_record))

                                if len(batch_data) == batch_size:
                                    try:
                                        await connection.executemany(insert_query, batch_data)
                                        batch_data.clear()
                                    except Exception as e:
                                        print(f"An error occurred while inserting the record: {e}")
                                        await connection.execute('ROLLBACK')

                            if batch_data:
                                try:
                                    await connection.executemany(insert_query, batch_data)
                                except Exception as e:
                                    print(f"An error occurred while inserting the record: {e}")
                                    await connection.execute('ROLLBACK')
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

    async def batch_upsert_dataframe(self, df, table_name, unique_columns, batch_size=250):
        try:
            async with lock:
                # Check if the table exists
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)
                
                # Copy DataFrame to avoid modifying the original and add a timestamp
                df = df.copy()
                df['insertion_timestamp'] = pd.to_datetime([datetime.now() for _ in range(len(df))])

                # Convert DataFrame to a list of dictionaries
                records = df.to_dict(orient='records')

                # Fetch column types from the table schema
                async with self.pool.acquire() as connection:
                    column_types = await connection.fetch(
                        f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
                    )
                    type_mapping = {col['column_name']: col['data_type'] for col in column_types}

                    # Prepare the SQL insert query with conflict handling to keep only the latest data
                    insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in df.columns)}) 
                    VALUES ({', '.join(f'${i}' for i in range(1, len(df.columns) + 1))})
                    ON CONFLICT ({', '.join(f'"{col}"' for col in unique_columns)})
                    DO UPDATE SET {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns if col not in unique_columns)}
                    """  # Only update non-unique columns

                    batch_data = []
                    for record in records:
                        new_record = []
                        for col, val in record.items():
                            pg_type = type_mapping.get(col)

                            # Handle data type conversions for PostgreSQL
                            if pd.isna(val):
                                new_record.append(None)
                            elif pg_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                                if isinstance(val, np.datetime64):
                                    new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))
                                elif isinstance(val, datetime):
                                    new_record.append(val)
                            elif pg_type in ['double precision', 'real'] and not isinstance(val, str):
                                new_record.append(float(val))
                            elif isinstance(val, np.int64):
                                new_record.append(int(val))
                            elif pg_type == 'integer' and not isinstance(val, int):
                                new_record.append(int(val))
                            else:
                                new_record.append(val)

                        batch_data.append(tuple(new_record))

                        # Insert data in batches
                        if len(batch_data) == batch_size:
                            try:
                                await connection.executemany(insert_query, batch_data)
                                batch_data = []  # Clear batch after successful insert
                            except Exception as e:
                                print(f"Error during batch insert: {e}")
                                await connection.execute('ROLLBACK')

                    # Insert any remaining records
                    if batch_data:
                        try:
                            await connection.executemany(insert_query, batch_data)
                        except Exception as e:
                            print(f"Error during final batch insert: {e}")
                            await connection.execute('ROLLBACK')

        except Exception as e:
            print(f"An error occurred: {e}")

            
    async def batch_upsert_dataframe_without_unique(self, df, table_name, unique_columns, batch_size=250):
        try:
            async with lock:
                # Check if the table exists
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)
                
                # Copy DataFrame to avoid modifying the original and add a timestamp
                df = df.copy()
                df['insertion_timestamp'] = pd.to_datetime([datetime.now() for _ in range(len(df))])

                # Convert DataFrame to a list of dictionaries
                records = df.to_dict(orient='records')

                # Fetch column types from the table schema using asyncpg
                async with self.pool.acquire() as connection:
                    column_types = await connection.fetch(
                        f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
                    )
                    type_mapping = {col['column_name']: col['data_type'] for col in column_types}

                    # Prepare the SQL insert query with conflict handling to keep only the latest data
                    insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(f'"{col}"' for col in df.columns)}) 
                    VALUES ({', '.join(f'${i}' for i in range(1, len(df.columns) + 1))})
                    """  # Only update non-unique columns

                    batch_data = []
                    for record in records:
                        new_record = []
                        for col, val in record.items():
                            pg_type = type_mapping.get(col)

                            # Handle data type conversions for PostgreSQL
                            if pd.isna(val):
                                new_record.append(None)
                            elif pg_type in ['timestamp', 'timestamp without time zone', 'timestamp with time zone']:
                                if isinstance(val, np.datetime64):
                                    new_record.append(pd.Timestamp(val).to_pydatetime().replace(tzinfo=None))
                                elif isinstance(val, datetime):
                                    new_record.append(val)
                            elif pg_type in ['double precision', 'real'] and not isinstance(val, str):
                                new_record.append(float(val))
                            elif isinstance(val, np.int64):
                                new_record.append(int(val))
                            elif pg_type == 'integer' and not isinstance(val, int):
                                new_record.append(int(val))
                            elif isinstance(val, list):  # Handling conditions list (array)
                                # Directly append Python list for asyncpg array handling
                                new_record.append(val)
                            else:
                                new_record.append(val)

                        batch_data.append(tuple(new_record))

                        # Insert data in batches
                        if len(batch_data) == batch_size:
                            try:
                                # asyncpg executemany supports batching
                                await connection.executemany(insert_query, batch_data)
                                batch_data = []  # Clear batch after successful insert
                            except Exception as e:
                                print(f"Error during batch insert: {e}")
                                await connection.execute('ROLLBACK')

                    # Insert any remaining records
                    if batch_data:
                        try:
                            await connection.executemany(insert_query, batch_data)
                        except Exception as e:
                            print(f"Error during final batch insert: {e}")
                            await connection.execute('ROLLBACK')

        except Exception as e:
            print(f"An error occurred: {e}")

    async def save_to_history(self, df, main_table_name, history_table_name):
        try:
            # Assume the DataFrame `df` contains the records to be archived
            if not await self.table_exists(history_table_name):
                await self.create_table(df, history_table_name, None)

            df['archived_at'] = datetime.now()  # Add an 'archived_at' timestamp
            await self.batch_insert_dataframe(df, history_table_name, None)
        except  Exception as e:
            print(e)
    async def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
  
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(query)
        return exists
    async def save_structured_messages(self, data_list: list[dict], table_name: str):
        if not data_list:
            return  # No data to insert

        # Assuming all dicts in data_list have the same keys
        fields = ', '.join(data_list[0].keys())
        values_placeholder = ', '.join([f"${i+1}" for i in range(len(data_list[0]))])
        values = ', '.join([f"({values_placeholder})" for _ in data_list])
        
        query = f'INSERT INTO {table_name} ({fields}) VALUES {values}'
       
        async with self.pool.acquire() as conn:
            try:
                flattened_values = [value for item in data_list for value in item.values()]
                await conn.execute(query, *flattened_values)
            except UniqueViolationError:
                print('Duplicate - Skipping')


    async def paginate_concurrent(self, url, as_dataframe=False, concurrency=25 ):
        """
        Concurrently paginates through polygon.io endpoints that contain the "next_url"

        Arguments:

        >>> URL: the endpoint to query

        >>> as_dataframe: bool - return as a dataframe or not. (default false)

        >>> concurrency: int - the number of session to run (default 25)
        
        """

        all_results = []

        

        pages_to_fetch = [url]
            
        while pages_to_fetch:
            tasks = []
            
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(self.fetch_page(next_url))
                
            results = await asyncio.gather(*tasks)
            if results is not None:
                for data in results:
                    if data is not None:
                        if "results" in data:
                            all_results.extend(data["results"])
                            
                        next_url = data.get("next_url")
                        if next_url:
                            next_url += f'&{urlencode({"apiKey": f"{self.api_key}"})}'
                            pages_to_fetch.append(next_url)
                    else:
                        break
        if as_dataframe:
            import pandas as pd
            return pd.DataFrame(all_results)
        else:
            return all_results
        



    #

    async def fetch_page(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
    

    async def fetch_endpoint(self, endpoint, params=None):
        """
        Uses endpoint / parameter combinations to fetch the polygon API

        dynamically.
        
        """
        # Build the query parameters
        filtered_params = {k: v for k, v in params.items() if v is not None} if params else {}
        
        if filtered_params:
            query_string = urlencode(filtered_params)
            if query_string:
                endpoint = f"{endpoint}?{query_string}&{self.api_key}"
                
        async with aiohttp.ClientSession() as session:
            async with session.get(url=endpoint, params=filtered_params) as response:
                response_data = await response.json()
                if 'next_url' in response_data:
                    # If "next_url" exists - the function auto paginates
                    return await self.paginate_concurrent(endpoint,as_dataframe=True,concurrency=40)
                return response_data  # or response.text() based on your needs
                

    
    async def get_option_contracts(self, ticker:str, 
                                   strike_price_greater_than:str=None, 
                                   strike_price_less_than:str=None, 
                                   expiry_date_greater_than:str=None, 
                                   expiry_date_less_than:str=None, limit:str='250'):
        """
        Returns options for a specified ticker.

        Arguments:

        >>> ticker: the ticker to query (required)

        >>> strike_price_greater_than: the minimum strike to be returned (optional)      


        >>> strike_price_less_than: the maximum strike to be returned (optional) 


        >>> expiry_greater_than: the minimum expiry date to be returned (optional)


        >>> expiry_less_than: the maximum expiry date to be returned (optional)

        >>> limit: the amount of contracts to be returned (max 250 per request)

        
        """


        endpoint = f"https://api.polygon.io/v3/snapshot/options/{ticker}"

        
        params = {
            "limit": limit,
            "apiKey": self.api_key
        }

        df = await self.fetch_endpoint(endpoint, params)
      
        # Columns to normalize
        columns_to_normalize = ['day', 'underlying_asset', 'details', 'greeks', 'last_quote', 'last_trade']

        # Apply the helper function to each row and each specified column
        for column in columns_to_normalize:
            df = df.apply(lambda row: flatten_nested_dict(row, column), axis=1)

        return df

    async def get_strike_thresholds(self, ticker:str, price):
        indices_list = ["SPX", "SPXW", "NDX", "VIX", "VVIX"]
        if price is not None and ticker in indices_list:
            lower_strike = round(float(price) * 0.97)
            upper_strike = round(float(price) * 1.03)
            return lower_strike, upper_strike
        else:
            lower_strike = round(float(price) * 0.85)
            upper_strike = round(float(price) * 1.15)
            return lower_strike, upper_strike

    async def get_near_the_money_options(self, ticker: str, exp_greater_than:str=None, exp_less_than:str=None):
        """
        Gets options near the money for a ticker
        """

        if exp_greater_than is None:
            exp_greater_than = self.today

        if exp_less_than is None:
            exp_less_than = self.eight_days_from_now



        price = await self.get_price(ticker)
  
        if price is not None:
            upper_strike, lower_strike = await self.get_strike_thresholds(ticker, price)
      
            async with aiohttp.ClientSession() as session:
                url = f"https://api.polygon.io/v3/snapshot/options/{ticker}?strike_price.lte={lower_strike}&strike_price.gte={upper_strike}&expiration_date.gte={exp_greater_than}&expiration_date.lte={exp_less_than}&limit=250&apiKey={self.api_key}"
         
                async with session.get(url) as resp:
                    r = await resp.json()
                    results = r['results'] if 'results' in r else None
                    if results is None:
                        return
                    else:
                        results = UniversalOptionSnapshot(results)
                       
                        tickers = results.ticker
                        if ticker is not None:
                            atm_tickers = ','.join(tickers)
                            return atm_tickers
                        else:
                            return None


    async def aggregate_volume_oi(self, ticker, start_date:str=None, end_date:str=None):
        if start_date == None:
            start_date = self._45_days_from_now

        if end_date == None:
            end_date = self._90_days_drom_now

        all_options = await self.get_near_the_money_data(ticker, exp_less_than=end_date, exp_greater_than=start_date)

        if all_options is not None:
            
            # Group by 'call_put' and then 'expiry'
            grouped = all_options.groupby(['cp', 'expiry'])
            print(grouped)

            aggregated_data = grouped.agg({
                'vol': 'sum',
                'strike': 'mean',
                'oi': 'sum',

            })
                    

            return aggregated_data
    async def get_near_the_money_data(self, ticker: str, exp_greater_than:str='2023-11-17', exp_less_than:str='2023-12-15'):
        """
        Gets options near the money for a ticker
        """





        price = await self.get_price(ticker)
  
        if price is not None:
            upper_strike, lower_strike = await self.get_strike_thresholds(ticker, price)
      
            async with aiohttp.ClientSession() as session:
                url = f"https://api.polygon.io/v3/snapshot/options/{ticker}?strike_price.lte={lower_strike}&strike_price.gte={upper_strike}&expiration_date.gte={exp_greater_than}&expiration_date.lte={exp_less_than}&limit=250&apiKey={self.api_key}"
         
                data = await self.paginate_concurrent(url)

                return UniversalOptionSnapshot(data).df
            


    async def get_universal_snapshot(self, ticker): #✅
        """Fetches the Polygon.io universal snapshot API endpoint"""
        url=f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=250&apiKey={self.api_key}"
        print(url)
  
        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            data = resp.json()
            results = data['results'] if 'results' in data else None
            if results is not None:
                return UniversalOptionSnapshot(results)
            



    async def get_option_chain_all(self, 
                                    underlying_asset: str, 
                                    strike_price: float = None, 
                                    strike_price_lte: float = None, 
                                    strike_price_gte: float = None, 
                                    expiration_date: str = None, 
                                    expiration_date_gte: str = None, 
                                    expiration_date_lte: str = None, 
                                    contract_type: str = None, 
                                    order: str = None, 
                                    limit: int = 250, 
                                    sort: str = None, 
                                    insert: bool = False):
            """
            Retrieve all options contracts for a specific underlying asset (ticker symbol) across multiple pages.
            
            This function paginates through all available data for a given ticker symbol and applies optional filters like 
            strike price, expiration date, and contract type. It returns a structured object with the fetched data and 
            optionally inserts the data into a database.

            :param underlying_asset: The underlying ticker symbol of the option contract (e.g., 'AAPL', 'SPX').
            :param strike_price: Exact strike price of the option contract.
            :param strike_price_lte: Strike price less than or equal to a specific value.
            :param strike_price_gte: Strike price greater than or equal to a specific value.
            :param expiration_date: Specific expiration date of the contract (YYYY-MM-DD).
            :param expiration_date_gte: Expiration date on or after a specific date (YYYY-MM-DD).
            :param expiration_date_lte: Expiration date on or before a specific date (YYYY-MM-DD).
            :param contract_type: Type of contract ('call' or 'put').
            :param order: Order results based on the specified field, either 'asc' or 'desc'.
            :param limit: Maximum number of results per API request (default 250, max allowed by API).
            :param sort: Field by which to sort the results (e.g., 'expiration_date').
            :param insert: If True, inserts the fetched data into the database (default False).
            :return: A UniversalOptionSnapshot object containing option chain data across all pages.
            
            :raises ValueError: If an invalid input parameter is provided.
            :raises Exception: For any failures in data retrieval or database interaction.
            """
            try:
                if not underlying_asset:
                    raise ValueError("Underlying asset ticker symbol must be provided.")

                # Handle special case for index assets (e.g., "I:SPX" for S&P 500 Index)
                if underlying_asset.startswith("I:"):
                    underlying_asset = underlying_asset.replace("I:", "")

                # Build query parameters
                params = {
                    'strike_price': strike_price,
                    'strike_price.lte': strike_price_lte,
                    'strike_price.gte': strike_price_gte,
                    'expiration_date': expiration_date,
                    'expiration_date.gte': expiration_date_gte,
                    'expiration_date.lte': expiration_date_lte,
                    'contract_type': contract_type,
                    'order': order,
                    'limit': limit,
                    'sort': sort
                }

                # Filter out None values
                params = {key: value for key, value in params.items() if value is not None}

                # Construct the API endpoint and query string
                endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
                
                if params:
                    query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
                    endpoint += '?' + query_string
                endpoint += f"&apiKey={self.api_key}"
                print(endpoint)
                # Log the API call for debugging purposes
                logging.debug(f"Fetching option chain data for {underlying_asset} with query: {params}")

                # Fetch the data using asynchronous pagination
                response_data = await self.paginate_concurrent(endpoint)

                # Parse response data into a structured option snapshot object
                option_data = UniversalOptionSnapshot(response_data)

                # Insert into the database if specified
                if insert == True:
                    logging.info("Inserting option chain data into the database.")
                    await self.connect()  # Ensure connection to the database
                    await self.batch_insert_dataframe(option_data.df, table_name='all_options', unique_columns='option_symbol')

                logging.info("Option chain data retrieval successful.")
                return option_data

            except ValueError as ve:
                logging.error(f"ValueError occurred: {ve}")
                

            except Exception as e:
                logging.error(f"An error occurred while fetching the option chain: {e}")
                
    async def get_option_chain_all_spx(self, 
                                    underlying_asset: str, 
                                    strike_price: float = None, 
                                    strike_price_lte: float = None, 
                                    strike_price_gte: float = None, 
                                    expiration_date: str = None, 
                                    expiration_date_gte: str = None, 
                                    expiration_date_lte: str = None, 
                                    contract_type: str = None, 
                                    order: str = None, 
                                    limit: int = 250, 
                                    sort: str = None, 
                                    insert: bool = False):
        """
        Retrieve all options contracts for a specific underlying asset (ticker symbol) across multiple pages.
        
        This function paginates through all available data for a given ticker symbol and applies optional filters like 
        strike price, expiration date, and contract type. It returns a structured object with the fetched data and 
        optionally inserts the data into a database.

        :param underlying_asset: The underlying ticker symbol of the option contract (e.g., 'AAPL', 'SPX').
        :param strike_price: Exact strike price of the option contract.
        :param strike_price_lte: Strike price less than or equal to a specific value.
        :param strike_price_gte: Strike price greater than or equal to a specific value.
        :param expiration_date: Specific expiration date of the contract (YYYY-MM-DD).
        :param expiration_date_gte: Expiration date on or after a specific date (YYYY-MM-DD).
        :param expiration_date_lte: Expiration date on or before a specific date (YYYY-MM-DD).
        :param contract_type: Type of contract ('call' or 'put').
        :param order: Order results based on the specified field, either 'asc' or 'desc'.
        :param limit: Maximum number of results per API request (default 250, max allowed by API).
        :param sort: Field by which to sort the results (e.g., 'expiration_date').
        :param insert: If True, inserts the fetched data into the database (default False).
        :return: A UniversalOptionSnapshot object containing option chain data across all pages.
        
        :raises ValueError: If an invalid input parameter is provided.
        :raises Exception: For any failures in data retrieval or database interaction.
        """
        try:
            if not underlying_asset:
                raise ValueError("Underlying asset ticker symbol must be provided.")


            # Build query parameters
            params = {
                'strike_price': strike_price,
                'strike_price.lte': strike_price_lte,
                'strike_price.gte': strike_price_gte,
                'expiration_date': expiration_date,
                'expiration_date.gte': expiration_date_gte,
                'expiration_date.lte': expiration_date_lte,
                'contract_type': contract_type,
                'order': order,
                'limit': limit,
                'sort': sort
            }

            # Filter out None values
            params = {key: value for key, value in params.items() if value is not None}

            # Construct the API endpoint and query string
            endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
            
            if params:
                query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
                endpoint += '?' + query_string
            endpoint += f"&apiKey={self.api_key}"
            print(endpoint)
            # Log the API call for debugging purposes
            logging.debug(f"Fetching option chain data for {underlying_asset} with query: {params}")

            # Fetch the data using asynchronous pagination
            response_data = await self.paginate_concurrent(endpoint)

            # Parse response data into a structured option snapshot object
            option_data = UniversalOptionSnapshot(response_data)

            # Insert into the database if specified
            if insert == True:
                logging.info("Inserting option chain data into the database.")
                await self.connect()  # Ensure connection to the database
                await self.batch_insert_dataframe(option_data.df, table_name='all_options', unique_columns='option_symbol')

            logging.info("Option chain data retrieval successful.")
            return option_data

        except ValueError as ve:
            logging.error(f"ValueError occurred: {ve}")
            

        except Exception as e:
            logging.error(f"An error occurred while fetching the option chain: {e}")
            


    async def get_all_market_options(self):
        endpoint = f"https://api.polygon.io/v3/snapshot?type=options&limit=250&apiKey={self.api_key}"
        response_data = await self.paginate_concurrent(endpoint)    

        return response_data



    async def find_symbols(self, underlying_asset, strike_price=None, strike_price_lte=None, strike_price_gte=None, expiration_date=None, expiration_date_gte=None, expiration_date_lite=None, contract_type=None, order=None, limit=250, sort=None):
        """
        Get all options contracts for an underlying ticker across all pages.

        :param underlying_asset: The underlying ticker symbol of the option contract.
        :param strike_price: Query by exact strike price of a contract.
        :param strike_price_lte: Query for strike price less than or equal to a value.
        :param strike_price_gte: Query for strike price greater than or equal to a value.
        :param expiration_date: Query by exact contract expiration with date format YYYY-MM-DD.
        :param expiration_date_gte: Query for expiration dates greater than or equal to a value.
        :param expiration_date_lite: Query for expiration dates less than or equal to a value.
        :param contract_type: Query by the type of contract (e.g., call or put).
        :param order: Order results based on the sort field (e.g., asc or desc).
        :param limit: Limit the number of results returned, default is 10 and max is 250.
        :param sort: Sort field used for ordering.
        :return: A list containing all option chain data across all pages.
        """
        params = {
            'strike_price': strike_price,
            'strike_price.lte': strike_price_lte,
            'strike_price.gte': strike_price_gte,
            'expiration_date': expiration_date,
            'expiration_date.gte': expiration_date_gte,
            'expiration_date.lte': expiration_date_lite,
            'contract_type': contract_type,
            'order': order,
            'limit': limit,
            'sort': sort
        }
        # Filter out None values
        params = {k: v for k, v in params.items() if v is not None}

        endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        if query_string:
            endpoint += '?' + query_string
        endpoint += f"&apiKey={self.api_key}"


        response_data = await self.paginate_concurrent(endpoint)
        option_data = UniversalOptionSnapshot(response_data)

        return option_data
    


    async def get_iv_skew(self, ticker, num_days:int):
        # Fetch all options data for the ticker
        days = (datetime.now() + timedelta(days=num_days)).strftime('%Y-%m-%d')
        all_options = await self.get_option_chain_all(ticker, expiration_date_gte=self.today, expiration_date_lte=days)

        # Create a DataFrame for easy manipulation
        options_df = pd.DataFrame({
            'strike': all_options.strike,
            'volume': all_options.volume,
            'cp': all_options.cp,
            'open_interest': all_options.open_interest,
            'implied_volatility': all_options.implied_volatility,
            'expiry': all_options.expiry,
        })


        # Find the strike with the lowest implied volatility per expiry
        lowest_iv_strikes = options_df.loc[options_df.groupby('expiry')['implied_volatility'].idxmin()]
        lowest_iv_strikes = lowest_iv_strikes[['expiry', 'strike', 'implied_volatility']]

        # Convert to dictionary format for easy lookup if needed
        lowest_iv_strikes_dict = lowest_iv_strikes.set_index('expiry').to_dict(orient='index')


        # Return bull and bear points totals, and lowest_iv_strikes for each expiry
        return lowest_iv_strikes_dict


    async def insert_multiple_option_data(self, strike, expiry, dte, time_value, moneyness, liquidity_score, cp, 
                                          exercise_style, option_symbol, theta, theta_decay_rate, delta, 
                                          delta_theta_ratio, gamma, gamma_risk, vega, vega_impact, timestamp, 
                                          oi, open, high, low, close, intrinstic_value, extrinsic_value, 
                                          leverage_ratio, vwap, conditions, price, trade_size, exchange, ask, 
                                          bid, spread, spread_pct, iv, bid_size, ask_size, vol, mid, 
                                          change_to_breakeven, underlying_price, ticker, return_on_risk, 
                                          velocity):
        insert_sql = """
        INSERT INTO opts (
            strike, expiry, dte, time_value, moneyness, liquidity_score, cp, 
            exercise_style, option_symbol, theta, theta_decay_rate, delta, 
            delta_theta_ratio, gamma, gamma_risk, vega, vega_impact, timestamp, 
            oi, open, high, low, close, intrinstic_value, extrinsic_value, 
            leverage_ratio, vwap, conditions, price, trade_size, exchange, ask, 
            bid, spread, spread_pct, iv, bid_size, ask_size, vol, mid, 
            change_to_breakeven, underlying_price, ticker, return_on_risk, 
            velocity
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
            $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41,
            $42, $43, $44
        )
        """
        data_tuples = list(zip(strike, expiry, dte, time_value, moneyness, liquidity_score, cp, 
                               exercise_style, option_symbol, theta, theta_decay_rate, delta, 
                               delta_theta_ratio, gamma, gamma_risk, vega, vega_impact, timestamp, 
                               oi, open, high, low, close, intrinstic_value, extrinsic_value, 
                               leverage_ratio, vwap, conditions, price, trade_size, exchange, ask, 
                               bid, spread, spread_pct, iv, bid_size, ask_size, vol, mid, 
                               change_to_breakeven, underlying_price, ticker, return_on_risk, 
                               velocity))

        async with self.pool.acquire() as conn:
            await conn.executemany(insert_sql, data_tuples)

    async def ensure_table_exists(self, conn, table_name, columns, column_types):
        # Combine column names and types into a list of strings
        column_definitions = ', '.join([f"{name} {dtype}" for name, dtype in zip(columns, column_types)])
        
        # Create a table if it does not exist (simple example, no primary key or other constraints)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}
        );
        """
        await conn.execute(create_table_query)



    async def yield_tickers(self):
        """
        An asynchronous generator that fetches and yields one ticker at a time from the database.

        Yields:
        str: A ticker from the database.
        """
        await self.connect()  # Ensure connection pool is ready
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Define our SELECT query
                query = 'SELECT option_symbol FROM options_data'
                # Obtain a cursor
                cursor = await conn.cursor(query)
                # Manually iterate over the cursor
                while True:
                    record = await cursor.fetchrow()
                    if record is None:
                        break
                    ticker = record['option_symbol']
                    yield ticker

    async def get_tickers(self):
        tickers = []
        await self.connect()  # Ensure connection pool is ready
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                query = 'SELECT option_symbol FROM options_data'
                cursor = await conn.cursor(query)
                # Fetch each record from the cursor
                record = await cursor.fetchrow()
                while record:
                    tickers.append(record['option_symbol'])
                    record = await cursor.fetchrow()
        return tickers

    async def fetch_iter(self, query, *params):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor(query):
                    yield record

    async def group_options_by_expiry(self, all_options: UniversalOptionSnapshot):
        """Takes all options and groups by expiration date."""
        


    async def chunk_250(self, all_options: UniversalOptionSnapshot):
        """Chunks all options into sizes of 250"""
        # Calculate the number of chunksF
        total_items = len(all_options.ticker)  # Replace with the actual length of all_options
        chunk_size = 250
        num_chunks = (total_items + chunk_size - 1) // chunk_size  # Calculate the total number of chunks

        # Iterate over each chunk
        for i in range(num_chunks):
            start_index = i * chunk_size
            end_index = start_index + chunk_size
            chunk = all_options[start_index:end_index]  # Get the current chunk

            # Process or print the current chunk
            yield chunk

   
    async def vol_oi_top_strike(self, ticker: str):
        """
        Gets the strikes with the highest volume and open interest across all expirations
        for both call and put options, returning specific columns.
        """

        all_options = await self.get_option_chain_all(ticker)
        all_options_df = all_options.df

        # Columns to return
        columns_to_return = ['ticker', 'strike', 'expiry', 'cp', 'oi', 'vol']

        # Create an empty DataFrame to store the final results
        final_df = pd.DataFrame()

        # Group by 'cp' (call/put) and 'expiry'
        grouped = all_options_df.groupby(['cp', 'expiry'])

        for (option_type, expiry), group in grouped:
            # Find the row with the max volume
            max_vol_row = group[group['vol'] == group['vol'].max()]

            # Find the row with the max open interest
            max_oi_row = group[group['oi'] == group['oi'].max()]

            # Append these rows to the final DataFrame
            # Selecting only the required columns
            final_df = pd.concat([final_df, max_vol_row[columns_to_return], max_oi_row[columns_to_return]])

        # Resetting the index of the final DataFrame
        final_df.reset_index(drop=True, inplace=True)

        return final_df

    async def lowest_theta(self, ticker: str):
        """
        Gets the option with the lowest theta for each expiry across all options,
        selecting the lowest out of the call or put for each expiry.
        """

        all_options = await self.get_option_chain_all(ticker)
        all_options_df = all_options.df

        # Ensure 'theta' is in numeric format
        all_options_df['theta'] = pd.to_numeric(all_options_df['theta'], errors='coerce')

        # Create an empty DataFrame to store the final results
        final_df = pd.DataFrame()

        # Group by 'expiry'
        grouped = all_options_df.groupby('expiry')

        for expiry, group in grouped:
            # Separate calls and puts
            calls = group[group['cp'] == 'call']
            puts = group[group['cp'] == 'put']

            # Find the row with the lowest theta for calls and puts
            min_theta_call = calls[calls['theta'] == calls['theta'].max()]
            min_theta_put = puts[puts['theta'] == puts['theta'].max()]

            # Select the one with the absolute lowest theta
            min_theta_option = pd.concat([min_theta_call, min_theta_put]).nsmallest(1, 'theta')

            # Append this row to the final DataFrame
            final_df = pd.concat([final_df, min_theta_option])

        # Resetting the index of the final DataFrame
        final_df.reset_index(drop=True, inplace=True)

        return final_df
    

    async def lowest_vega(self, ticker: str):
        """
        Gets the option with the lowest vega for each expiry across all options,
        selecting the lowest out of the call or put for each expiry.
        """

        all_options = await self.get_option_chain_all(ticker)
        all_options_df = all_options.df

        # Ensure 'theta' is in numeric format
        all_options_df['vega'] = pd.to_numeric(all_options_df['vega'], errors='coerce')

        # Create an empty DataFrame to store the final results
        final_df = pd.DataFrame()

        # Group by 'expiry'
        grouped = all_options_df.groupby('expiry')

        for expiry, group in grouped:
            # Separate calls and puts
            calls = group[group['cp'] == 'call']
            puts = group[group['cp'] == 'put']

            # Find the row with the lowest theta for calls and puts
            min_theta_call = calls[calls['vega'] == calls['vega'].max()]
            min_theta_put = puts[puts['vega'] == puts['vega'].max()]

            # Select the one with the absolute lowest theta
            min_theta_option = pd.concat([min_theta_call, min_theta_put]).nsmallest(1, 'vega')

            # Append this row to the final DataFrame
            final_df = pd.concat([final_df, min_theta_option])

        # Resetting the index of the final DataFrame
        final_df.reset_index(drop=True, inplace=True)

        return final_df

    async def highest_velocity(self, ticker: str):
        """
        Gets the option with the highest velocity for each expiry across all options,
        selecting the highest out of the call or put for each expiry.
        """

        all_options = await self.get_option_chain_all(ticker)
        all_options_df = all_options.df

        # Ensure 'velocity' is in numeric format
        all_options_df['velocity'] = pd.to_numeric(all_options_df['velocity'], errors='coerce')

        # Create an empty DataFrame to store the final results
        final_df = pd.DataFrame()

        # Group by 'expiry'
        grouped = all_options_df.groupby('expiry')

        for expiry, group in grouped:
            # Separate calls and puts
            calls = group[group['cp'] == 'call']
            puts = group[group['cp'] == 'put']

            # Find the row with the highest velocity for calls and puts
            max_velocity_call = calls[calls['velocity'] == calls['velocity'].max()]
            max_velocity_put = puts[puts['velocity'] == puts['velocity'].max()]

            # Select the one with the absolute highest velocity
            max_velocity_option = pd.concat([max_velocity_call, max_velocity_put]).nlargest(1, 'velocity')

            # Append this row to the final DataFrame
            final_df = pd.concat([final_df, max_velocity_option])

        # Resetting the index of the final DataFrame
        final_df.reset_index(drop=True, inplace=True)

        return final_df


    def binomial_american_option(self, S, K, T, r, sigma, N, option_type='put'):
        """
        Binomial model for American option pricing.

        :param S: Current stock price.
        :param K: Strike price.
        :param T: Time to expiry in years.
        :param r: Risk-free interest rate.
        :param sigma: Volatility of the stock.
        :param N: Number of steps in the binomial tree.
        :param option_type: Type of the option ('call' or 'put').
        :return: Estimated option price.
        """
        dt = T / N  # Time step
        u = np.exp(sigma * np.sqrt(dt))  # Upward movement factor
        d = 1 / u  # Downward movement factor
        p = (np.exp(r * dt) - d) / (u - d)  # Risk-neutral probability

        # Initialize asset price at maturity
        ST = np.zeros((N+1, N+1))
        ST[0, 0] = S

        # Create the binomial tree
        for i in range(1, N + 1):
            ST[i, 0] = ST[i - 1, 0] * u
            for j in range(1, i + 1):
                ST[i, j] = ST[i - 1, j - 1] * d

        # Initialize option values at maturity
        option = np.zeros((N+1, N+1))
        for j in range(N+1):
            if option_type == 'call':
                option[N, j] = max(0, ST[N, j] - K)
            else:
                option[N, j] = max(0, K - ST[N, j])

        # Step backwards through the tree
        for i in range(N-1, -1, -1):
            for j in range(i + 1):
                option_early_exercise = max(ST[i, j] - K, 0) if option_type == 'call' else max(K - ST[i, j], 0)
                option[i, j] = max(option_early_exercise, np.exp(-r * dt) * (p * option[i + 1, j] + (1 - p) * option[i + 1, j + 1]))

        return option[0, 0]

    async def get_theoretical_price(self, ticker):
        # Fetching necessary data
        prices = self.yf.fast_info(ticker)
        current_price = prices[prices[0] == 'lastPrice'][1].values[0]
    
        all_options_data = await self.get_option_chain_all(underlying_asset=ticker)

        risk_free_rate = 0.0565  # Current risk-free rate (5.65%)
        N = 100  # Number of steps in the binomial tree

        theoretical_values = []

    

        for bid, ask, iv, strike, expiry, option_type, dtes, volume, oi, velocity, delta, vega, gamma, theta, close in zip(
                all_options_data.bid, all_options_data.ask, all_options_data.implied_volatility, 
                all_options_data.strike, all_options_data.expiry, all_options_data.contract_type, all_options_data.days_to_expiry, all_options_data.open_interest, all_options_data.volume, all_options_data.option_velocity, all_options_data.delta, all_options_data.vega, all_options_data.gamma, all_options_data.theta, all_options_data.underlying_price):
            def convert_days_to_years(days):
                # Assuming 365 days in a year
                return float(days) / 365.0

            T = convert_days_to_years(dtes)
            
            # Check if implied volatility is None
            if iv is None:
                # Handle the case where implied volatility is not available.
                # You might want to skip this option, use an average IV, or another method.
                continue
            if delta is None:
                continue
            if gamma is None:
                continue
            if theta is None:
                continue
            if vega is None:
                continue

            sigma = iv  # Implied volatility

            # Calculate theoretical price using the binomial model
            theoretical_price = self.binomial_american_option(current_price, strike, T, risk_free_rate, sigma, N, option_type)
            
            theoretical_values.append({
                'ticker': ticker,
                'current_price': close,
                'iv': iv,
                'velocity': velocity,
                'strike': strike,
                'expiry': expiry,
                'bid': bid,
                'theoretical_price': theoretical_price,
                'ask': ask,
                'volume': volume,
                'oi': oi,
                'delta': delta,
                'gamma': gamma,
                'vega': vega,
                'theta': theta,
                'type': option_type
            })

        return theoretical_values
    async def gpt_filter(self, ticker:str=None, strike:float=None, strike_min:float=None, strike_max:float=None, cp:str=None, expiry:str=None, expiry_min:str=None, expiry_max:str=None, dte_max:int=None, dte_min:int=None, 
                liquidity_score_min:float=None, liquidity_score_max:float=None, theta_min:float=None, theta_max:float=None, 
                delta_min:float=None, delta_max:float=None, gamma_min:float=None, gamma_max:float=None, vega_min:float=None, 
                vega_max:float=None, timestamp_min:float=None, timestamp_max:float=None, oi_min:float=None, oi_max:float=None, 
                open_min:float=None, open_max:float=None, high_min:float=None, high_max:float=None, low_min:float=None, 
                low_max:float=None, close_min:float=None, close_max:float=None, intrinsic_value_min:float=None, 
                intrinsic_value_max:float=None, extrinsic_value_min:float=None, extrinsic_value_max:float=None, 
                leverage_ratio_min:float=None, leverage_ratio_max:float=None, vwap_min:float=None, vwap_max:float=None, 
                price_min:float=None, price_max:float=None, trade_size_min:float=None, trade_size_max:float=None, 
                ask_min:float=None, ask_max:float=None, bid_min:float=None, bid_max:float=None, spread_min:float=None, 
                spread_max:float=None, iv_min:float=None, iv_max:float=None, mid_min:float=None, mid_max:float=None, 
                underlying_price_min:float=None, underlying_price_max:float=None, return_on_risk_min:float=None, 
                return_on_risk_max:float=None, velocity_min:float=None, velocity_max:float=None, sensitivity_min:float=None, 
                sensitivity_max:float=None, greeks_balance_min:float=None, greeks_balance_max:float=None, 
                opp_min:float=None, opp_max:float=None, exchange:str=None, bid_size_min:float=None, bid_size_max:float=None, ask_size_min:float=None, ask_size_max:float=None, vol_min:float=None, vol_max:float=None):
        """Returns table chunks of filtered options!"""
        await self.connect()

        # Manually construct the params dictionary
        params = locals()
        params = {k: v for k, v in params.items() if v is not None and k != 'opts'}

        records = await self.filter_options(**params)

        # Process and print the fetched records
        df = pd.DataFrame(records)
        table = tabulate(df, headers=['sym', 'strike', 'cp', 'expiry', 'theta'], tablefmt='fancy', showindex=False)
        # Break apart data into chunks of 4000 characters
        chunks = chunk_string(table, 4000)
        # Fetch records

        return chunks

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None



    async def all_options_snapshot(self):
        endpoint = f"https://api.polygon.io/v3/snapshot?type=options&apiKey={self.api_key}&limit=250"


        data = await self.paginate_concurrent(endpoint)
        print(data)
        data = data['results'] if 'results' in data else None
        break_even_price = [i.get('break_even_price') for i in data]
        session = [i.get('session') for i in data]
        details = [i.get('details') for i in data]
        greeks = [i.get('greeks') for i in data]
        delta = [i.get('delta') for i in greeks]
        gamma = [i.get('gamma') for i in greeks]
        theta = [i.get('theta') for i in greeks]
        vega = [i.get('vega') for i in greeks]
        last_quote = [i.get('last_quote') for i in data]
        ask = [i.get('ask') for i in last_quote]
        bid = [i.get('bid') for i in last_quote]
        ask_ex = [i.get('ask_exchange') for i in last_quote]
        bid_ex = [i.get('bid_exchange') for i in last_quote]
        bid_size = [i.get('bid_size') for i in last_quote]
        ask_size = [i.get('ask_size') for i in last_quote]
        
        last_trade = [i.get('last_trade') for i in data]
        open_interest = [i.get('open_interest') for i in data]
        underlying_asset = [i.get('underlying_asset') for i in data]
        name = [i.get('name') for i in data]
        market_status = [i.get('market_status') for i in data]
        ticker = [i.get('ticker') for i in data]
        type = [i.get('type') for i in data]
        print(underlying_asset)
        print(details)



    async def working_universal(self, symbol_string):
        """"""

        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://api.polygon.io/v3/snapshot?ticker.any_of={symbol_string}&limit=250&apiKey={self.api_key}")


            data = data.json()


            results = data['results']
            if results:

                return WorkingUniversal(results)
            

    async def get_table_columns(self, table):
        """
        Fetches the column names of a given table dynamically and returns them as a list.

        :param table: The name of the table.
        :param connection_string: The database connection string.
        :return: A list of column names.
        """
        try:
            # Create a connection to the database
            conn = await asyncpg.connect("postgresql://chuck:fud@localhost:5432/market_data")

            # Query to get the column names
            query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position;
            """

            # Execute the query and fetch results
            rows = await conn.fetch(query, table)

            # Close the connection
            await conn.close()

            # Extract column names from the query result
            columns = [row['column_name'] for row in rows]
            return columns

        except Exception as e:
            print(f"An error occurred: {e}")
            return []