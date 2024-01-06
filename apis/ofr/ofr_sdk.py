import aiohttp
from asyncpg import create_pool
from asyncpg.exceptions import UndefinedTableError
from .ofr_list_sets import NYPD_OFR,FNYR_OFR,MMF_OFR,REPO_OFR,TYLD_OFR
import pandas as pd
import os



db_config = {
    "host": os.environ.get('DB_HOST'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('DB_PASSWORD'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME') # Database name for the new jawless database
}



class OFR:
    def __init__(self, connection_string= None):
        self.base_url = "https://data.financialresearch.gov/v1"


        self.endpoints = { 
            'mnemonics': '/metadata/mnemonics',
            'metadata_query': '/metadata/query',
            'metadata_search': '/metadata/search',
            'series_data': '/series/timeseries',
            'series_spread': '/calc/spread',
            'full_series': '/series/full',
            'multifull_series': '/series/multifull',
            'dataset': '/series/dataset'
        }
        self.conn = None
        self.pool = None
        if connection_string is not None:
            self.connection_string = os.environ.get('CONNECTION_STRING')

        self.chat_memory = []  # In-memory list to store chat messages
        self.offset = 0


    async def connect(self):
        # Assuming create_pool is defined somewhere and db_config is provided
        self.pool = await create_pool(
            **db_config, min_size=1, max_size=10
        )
        return self.pool


    async def construct_series_url(
        self,
        mnemonic,
        label="aggregation",
        start_date=None,
        end_date=None,
        periodicity=None,
        how=None,
        remove_nulls=None,
        time_format=None,
    ):
        base_url = "https://data.financialresearch.gov/v1/series/timeseries"
        
        # Initialize query parameters
        params = {}
        
        # Required parameter
        params['mnemonic'] = mnemonic
        
        # Optional parameters
        if label:
            params['label'] = label
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        if periodicity:
            params['periodicity'] = periodicity
        if how:
            params['how'] = how
        if remove_nulls:
            params['remove_nulls'] = remove_nulls
        if time_format:
            params['time_format'] = time_format
            
        # Construct the full URL
        query_string = "&".join(f"{key}={value}" for key, value in params.items())
        full_url = f"{base_url}?{query_string}"
        
        return full_url
    # Function to generate URLs for all mnemonics in a given dictionary
    async def generate_urls_for_mnemonics(self, mnemonic_dict, **kwargs):
        urls = {}
        for mnemonic, series_name in mnemonic_dict.items():
            url = self.construct_series_url(mnemonic, **kwargs)
            urls[series_name] = url
        return urls


    async def query_ofr_api(self, endpoint):
        print(self.base_url+endpoint)
        async with aiohttp.ClientSession() as session:
            async with session.get(self.base_url+endpoint) as resp:
                mnemonics_list = await resp.json()

                return mnemonics_list
    async def insert_ofr_data(self, df: pd.DataFrame):
        async with self.pool.acquire() as conn:
            df['date'] = pd.to_datetime(df['date'])
            df['value'] = df['value'].astype(float)
            try:
                await conn.executemany(
                    """
                    INSERT INTO ofr_data (
                        date, 
                        value, 
                        name, 
                        mnemonic
                    ) VALUES ($1, $2, $3, $4)
                    ON CONFLICT (date, mnemonic) DO UPDATE
                    SET value = EXCLUDED.value, name = EXCLUDED.name;
                    """,
                    list(df.itertuples(index=False, name=None))
                )
            except UndefinedTableError:
                print(f"No table has been created! Creating the table now..")

                # Create the table
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ofr_data (
                        date DATE NOT NULL,
                        value FLOAT NOT NULL,
                        name TEXT NOT NULL,
                        mnemonic TEXT NOT NULL,
                        PRIMARY KEY (date, mnemonic)
                    );
                    """
                )

                # Try inserting again
                await conn.executemany(
                    """
                    INSERT INTO ofr_data (
                        date, 
                        value, 
                        name, 
                        mnemonic
                    ) VALUES ($1, $2, $3, $4)
                    ON CONFLICT (date, mnemonic) DO UPDATE
                    SET value = EXCLUDED.value, name = EXCLUDED.name;
                    """,
                    list(df.itertuples(index=False, name=None))
                )

                print(f'Table created successfully.')