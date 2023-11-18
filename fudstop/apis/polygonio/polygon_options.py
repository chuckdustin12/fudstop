import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from dotenv import load_dotenv
load_dotenv()
import os
import sys
import pandas as pd
from pathlib import Path
import aiohttp
import asyncpg
import asyncio
from asyncpg.exceptions import UniqueViolationError
from asyncpg import create_pool
from asyncio import Lock
from urllib.parse import urlencode
from datetime import datetime, timedelta
from ..webull.webull_trading import WebullTrading
import numpy as np
from .models.option_models.universal_snapshot import UniversalOptionSnapshot
from .polygon_helpers import flatten_nested_dict, flatten_dict
lock = Lock()
trading = WebullTrading()
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
class PolygonOptions:
    def __init__(self, host, port, user, password, database):
        self.conn = None
        self.pool = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection_string = os.environ.get('POLYGON_STRING')

        self.database = os.environ.get('DB_NAME')

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



        self.db_config = {
            "host": os.environ.get('DB_HOST', 'localhost'), # Default to this IP if 'DB_HOST' not found in environment variables
            "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
            "user": os.environ.get('DB_USER', 'postgres'), # Default to 'postgres' if 'DB_USER' not found
            "password": os.environ.get('DB_PASSWORD', 'fud'), # Use the password from environment variable or default
            "database": os.environ.get('DB_NAME', 'polygon') # Database name for the new jawless database
        }




    async def connect(self):
       
        self.pool = await asyncpg.create_pool(**self.db_config)

        return self.pool
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


    async def option_trades(self, ticker:str,as_dataframe:bool=False):
        """
        Gets option trades for an option, or stock.

        >>> ticker: the ticker to query 


        >>> limit: the number of trades to return. (default 50000)
        

        >>> as_dataframe: whether to return as a dataframe or not (default false)
        """
        url = f"https://api.polygon.io/v3/trades/{ticker}?limit=50000&apiKey={self.api_key}"
        print(url)
    

        data = await self.paginate_concurrent(url, as_dataframe=True)
        if data is not None:
            return data

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


       

        results = await self.paginate_concurrent(url, as_dataframe=False, concurrency=50)
        if as_dataframe == True:
            return pd.DataFrame(results)
        return results
            

    async def batch_insert_dataframe(self, df, table_name, unique_columns, batch_size=250):
        """
        WORKS - Creates table - inserts data based on DTYPES.
        
        """
     
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


    async def paginate_concurrent(self, url, as_dataframe=False, concurrency=25):
        all_results = []

        
        async with aiohttp.ClientSession() as session:
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
                
    async def get_price(self, ticker:str):
        """
        Fetches price from Webull API to use for option queries

        Arguments:

        >>> ticker: required - the ticker to survey
        """

        if ticker.startswith('I:'):
            ticker = ticker.replace('I:', '')
        datas = await trading.stock_quote(ticker)

        price = datas.web_stock_close

        return price

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
            
    async def get_skew(self, ticker:str):
        """Gets the IV skew across all expiration dates along with the strikes above and below the lowest IV.
        
        Arguments:
        >>> ticker: required - the ticker to survey
        """

        options = await self.get_option_contracts(ticker, expiry_date_less_than='2023-11-03')

        # Sort by 'expiration_date' and 'implied_volatility'
        options_sorted = options.sort_values(by=['expiration_date', 'implied_volatility'])

        results = []

        # For each expiration date, get the lowest IV strike and the ones above and below it
        for expiration, group in options_sorted.groupby('expiration_date'):
            min_iv_idx = group['implied_volatility'].idxmin()
            
            # Get rows for lowest IV and the ones immediately above and below it
            rows = group.loc[[min_iv_idx - 1, min_iv_idx, min_iv_idx + 1], :]
            
            results.append(rows)

        # Concatenate all the results
        result_df = pd.concat(results)

     
        return result_df


    async def get_universal_snapshot(self, ticker, retries=3): #✅
        """Fetches the Polygon.io universal snapshot API endpoint"""
        timeout = aiohttp.ClientTimeout(total=10)  # 10 seconds timeout for the request
        
        for retry in range(retries):
        # async with sema:
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&apiKey={self.api_key}&limit=250"

            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        results = data.get('results', None)
        
                        if results is not None:
                            flattened_results = [flatten_dict(result) for result in results]
                            return flattened_results
                            
                except aiohttp.ClientConnectorError:
                    print("ClientConnectorError occurred. Retrying...")
                    continue
                
                except aiohttp.ContentTypeError as e:
                    print(f"ContentTypeError occurred: {e}")  # Consider logging this
                    continue
                
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")  # Consider logging this
                    continue


    async def get_option_chain_all(self, underlying_asset, strike_price=None, expiration_date=None, contract_type=None, order=None, limit=250, sort=None):
        """
        Get all options contracts for an underlying ticker across all pages.

        :param underlying_asset: The underlying ticker symbol of the option contract.
        :param strike_price: Query by strike price of a contract.
        :param expiration_date: Query by contract expiration with date format YYYY-MM-DD.
        :param contract_type: Query by the type of contract.
        :param order: Order results based on the sort field.
        :param limit: Limit the number of results returned, default is 10 and max is 250.
        :param sort: Sort field used for ordering.
        :return: A list containing all option chain data across all pages.
        """
        endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}?limit=250&apiKey={self.api_key}"

        async with aiohttp.ClientSession() as session:
            response_data = await self.paginate_concurrent(endpoint)
          
            option_data = UniversalOptionSnapshot(response_data)

            return option_data
        

 

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



    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None
