import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from _markets.list_sets.ticker_lists import most_active_tickers
import os
from dotenv import load_dotenv
import asyncio
from asyncio import Lock
import asyncpg
import aiohttp
import pandas as pd
from .oic_models import OICOptionsMonitor
load_dotenv()
import requests
lock = Lock()
from datetime import datetime
import numpy as np

session = requests.session()
class AsyncOICSDK:
    def __init__(self, host, port, user, password, database):
        self.refresh_key_payload = {
            "clientKey": f"{os.environ.get('YOUR_OIC_KEY')}",
            "clientName": "OIC_test"
        }
        self.conn = None
        self.pool = None
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.token = None
        self.initialize_token()
        self.db_config = {
            "host": os.environ.get('DB_HOST', 'localhost'), # Default to this IP if 'DB_HOST' not found in environment variables
            "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
            "user": os.environ.get('DB_USER', 'postgres'), # Default to 'postgres' if 'DB_USER' not found
            "password": os.environ.get('DB_PASSWORD', 'fud'), # Use the password from environment variable or default
            "database": os.environ.get('DB_NAME', 'oic') # Database name for the new jawless database
        }
    def initialize_token(self):
        """
        Initialize the token by making the first request.
        """
        r = session.post(
            "https://ivlivefront.ivolatility.com/token/client/get",
            json=self.refresh_key_payload
        )
        if r.status_code == 200:
            self.token = r.text # Adjust the key according to actual response structure

    def build_headers(self):
        """
        Build headers using the current token.
        """
        if not self.token:
            self.initialize_token()
        return {'Authorization': f'Bearer {self.token}'}

    def refresh_token(self):
        """
        Refresh the token.
        """
        r = session.post(
            "https://ivlivefront.ivolatility.com/token/client/get",
            headers=self.build_headers(),
            json=self.refresh_key_payload
        )
        if r.status_code == 200:
            self.token = r
        return self.token
    async def get_option_id(self, ticker):
        """
        Gets the option ID using the lookup endpoint
        
        """
        url = f"https://private-authorization.ivolatility.com/lookup/?sortField=SYMBOL&symbol={ticker}&region=1&matchingType=EXACTLY&pageSize=10&page=0"
        async with aiohttp.ClientSession(headers=self.build_headers()) as session:
            async with session.get(url) as resp:
                try:
                    r = await resp.json()
                    print(r)
                    pages = r['page']
                    id = pages[0]['stockId']
                    return id
                except Exception as e:
                    print(e)
                    return None
    
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


    async def most_active_options(self):
        """
        Gets the most active options from the Options Industry Council
        
        """
        url=f"https://private-authorization.ivolatility.com/favorites/instruments/most-active"
        data = await session.get(url, headers=self.build_headers()).json()


        print(data)

        df = pd.DataFrame(data)
        return df

    async def get_price(self, ticker):
        url = f"https://api.polygon.io/v2/last/nbbo/{ticker}?apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
        async with aiohttp.ClientSession(headers=self.build_headers()) as session:
            async with session.get(url) as resp:
                resp = await resp.json()
                results = resp['results'] if 'results' in resp else None
                if results is not None:
                    price = results['P']
                    print(price)
                    return price

    async def options_monitor(self, ticker):
        stock_id = await self.get_option_id(ticker)
        if stock_id is not None:
            stock_price = await self.get_price(ticker)
            print(stock_price)
            if stock_price is not None:
            
                url = f"https://private-authorization.ivolatility.com/options-monitor/listOptionDataRow?stockId={stock_id}&center={stock_price}&regionId=1&strikesN=75&columns=alpha&columns=ask&columns=asksize&columns=asktime&columns=bid&columns=bidsize&columns=bidtime&columns=change&columns=changepercent&columns=delta&columns=theoprice&columns=gamma&columns=ivint&columns=ivask&columns=ivbid&columns=mean&columns=openinterest_eod&columns=optionsymbol&columns=volume&columns=paramvolapercent_eod&columns=alpha_eod&columns=ask_eod&columns=bid_eod&columns=delta_eod&columns=theoprice_eod&columns=gamma_eod&columns=ivint_eod&columns=mean_eod&columns=rho_eod&columns=theta_eod&columns=vega_eod&columns=changepercent_eod&columns=change_eod&columns=volume_eod&columns=quotetime&columns=rho&columns=strike&columns=style&columns=theta&columns=vega&columns=expirationdate&columns=forwardprice&columns=forwardprice_eod&columns=days&columns=days_eod&columns=iv&columns=iv_eod&rtMode=RT&userId=9999999&uuid=null"
                async with aiohttp.ClientSession(headers=self.build_headers()) as session:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        print(data)
                        data = OICOptionsMonitor(resp)
                        if data is not None:
                            df = data.as_dataframe
                            await self.batch_insert_dataframe(df, table_name='monitor', unique_columns='put_optionsymbol, call_optionsymbol')

                    

                    return data
            


    async def monitor_all(self):
        tasks = [self.options_monitor(i) for i in most_active_tickers]


        await asyncio.gather(*tasks)
            




