import os
import time
import sys
import re
import pytz
import httpx
import aiohttp
from typing import List, AsyncGenerator, Tuple, Optional, Dict, Any
import asyncpg
import logging
import numpy as np
import pandas as pd
import asyncio
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from asyncio import Lock
from tabulate import tabulate
from dotenv import load_dotenv
from more_itertools import chunked
from asyncpg.exceptions import UniqueViolationError

# Local imports (as in your original codebase)
from fudstop4.apis.helpers import convert_to_eastern_time
from fudstop4.apis.polygonio.mapping import OPTIONS_EXCHANGES
from fudstop4._markets.list_sets.dicts import option_conditions
from fudstop4.all_helpers import chunk_string

# Models
from .models.technicals import RSI
from .models.option_models.option_snapshot import WorkingUniversal
from .models.option_models.universal_snapshot import (
    UniversalOptionSnapshot,
    UniversalOptionSnapshot2,
    SpxSnapshot
)
from .polygon_helpers import (
    get_human_readable_string,
    flatten_nested_dict,
    flatten_dict
)

load_dotenv()

# Lock for batch inserts/updates to avoid race conditions
lock = Lock()

# A semaphore to limit concurrency (adjust as needed)
sema = asyncio.Semaphore(4)

def dtype_to_postgres(dtype):
    """
    Maps Pandas dtypes to PostgreSQL types for table creation.
    """
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

def convert_ns_to_est(ns_timestamp):
    """
    Converts a Unix nanosecond timestamp to an EST date string.
    """
    timestamp_in_seconds = ns_timestamp / 1e9
    utc_time = datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)
    est_time = utc_time.astimezone(pytz.timezone('US/Eastern'))
    est_time = est_time.replace(tzinfo=None)  # Remove timezone info
    return est_time.strftime("%Y-%m-%d %H:%M:%S")
# ---------------------------------------------------------------------------
# Helper coroutine that runs a coroutine with a timeout and returns a tuple (result, elapsed, error)
# ---------------------------------------------------------------------------
async def timed_safe(coro, timeout: int = 10) -> Tuple[Any, float, Optional[Exception]]:
    start = time.perf_counter()
    try:
        result = await asyncio.wait_for(coro, timeout=timeout)
        elapsed = time.perf_counter() - start
        return result, elapsed, None
    except Exception as e:
        elapsed = time.perf_counter() - start
        return None, elapsed, e

# ---------------------------------------------------------------------------
# Helper to run a coroutine under a semaphore with timeout.
# ---------------------------------------------------------------------------
async def sem_timed(coro, semaphore: asyncio.Semaphore, timeout: int = 10) -> Tuple[Any, float, Optional[Exception]]:
    async with semaphore:
        return await timed_safe(coro, timeout=timeout)

# ---------------------------------------------------------------------------
# Helper to inspect an object's attributes.
# ---------------------------------------------------------------------------
def inspect_object(obj: Any) -> Any:
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    elif hasattr(obj, "__slots__"):
        return {slot: getattr(obj, slot) for slot in obj.__slots__}
    else:
        return str(obj)

class PolygonOptions:
    """
    A class providing Polygon.io integration for fetching, filtering, and saving
    option data into a PostgreSQL database via asyncpg and httpx/aiohttp.
    """

    def __init__(
        self,
        user: str = 'chuck',
        database: str = 'fudstop3',
        host: str = 'localhost',
        port: int = 5432,
        password: str = 'fud'
    ):
        """
        Initialize database connection parameters and other essential fields.
        """
        self.user = user
        self.database = database
        self.host = host
        self.port = port
        self.password = password
        self.conn: asyncpg.Connection | None = None


        self.pool = None
        self.session = None
        self.http_session = None  # For aiohttp session usage

        self.api_key = os.environ.get('YOUR_POLYGON_KEY', '')
        if not self.api_key:
            logging.warning("No Polygon API key found in environment.")

        # Commonly used date references
        now = datetime.now()
        self.today = now.strftime('%Y-%m-%d')
        self.yesterday = (now - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (now + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (now - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (now + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (now - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (now + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_ago = (now - timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (now + timedelta(days=8)).strftime('%Y-%m-%d')
        self.one_year_from_now = (now + timedelta(days=365)).strftime('%Y-%m-%d')
        self.one_year_ago = (now - timedelta(days=365)).strftime('%Y-%m-%d')
        self._45_days_from_now = (now + timedelta(days=45)).strftime('%Y-%m-%d')
        self._90_days_drom_now = (now + timedelta(days=90)).strftime('%Y-%m-%d')

        # Optional usage if you want a direct connection string
        self.connection_string = (
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

        self.db_config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "database": self.database
        }

    ########################################################################
    # AIOHTTP session handling
    ########################################################################
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.session:
            await self.session.close()
    async def create_http_session(self):
        """
        Asynchronous method to create an aiohttp session if none exists or if closed.
        """
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession()
        return self.http_session
    async def fetchval(self, query: str, *args):
        """
        Fetch a single scalar value (first column of the first row).
        Works like asyncpg.fetchval().
        """
        try:
            async with self.pool.acquire() as conn:
                # Use asyncpg's built-in fetchval when args exist
                if args:
                    return await conn.fetchval(query, *args)

                # When no args, emulate fetchval manually
                row = await conn.fetchrow(query)
                if row is None:
                    return None
                return row[0]  # first column
        except Exception as e:
            logging.error(e)
            return None
    async def get_http_session(self):
        """
        Asynchronous method to get the existing aiohttp session or create a new one.
        """
        return (
            self.http_session
            if self.http_session and not self.http_session.closed
            else await self.create_http_session()
        )

    async def close_http_session(self):
        """
        Asynchronous method to close an aiohttp session.
        """
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            self.http_session = None

    ########################################################################
    # PostgreSQL Connection Handling
    ########################################################################

    async def connect(self):
        """
        Initializes the connection pool if it doesn't already exist.
        """
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
    async def add_column_to_table(self, table_name: str, column_name: str, column_type: str, default_value=None):
        """
        Adds a new column to an existing table if it does not already exist.

        Args:
            table_name (str): The name of the table to modify.
            column_name (str): The name of the new column to add.
            column_type (str): The data type of the new column (e.g., TEXT, INTEGER, BOOLEAN).
            default_value (optional): Default value for the new column (if needed).
        
        Example:
            await add_column_to_table("users", "is_active", "BOOLEAN", default_value="TRUE")
        """
        try:
            async with self.pool.acquire() as conn:
                # Check if the column already exists
                check_query = f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = $1 AND column_name = $2;
                """
                existing_column = await conn.fetchval(check_query, table_name, column_name)

                if existing_column:
                    logging.info(f"⚠️ Column '{column_name}' already exists in '{table_name}'. Skipping addition.")
                    return False  # Column already exists

                # Build the ALTER TABLE query
                alter_query = f"""
                    ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}
                    {"DEFAULT " + str(default_value) if default_value is not None else ""};
                """
                await conn.execute(alter_query)
                logging.info(f"✅ Successfully added column '{column_name}' to '{table_name}'.")
                return True

        except asyncpg.exceptions.PostgresError as e:
            if "42703" in str(e):  # PostgreSQL undefined_column error code
                logging.warning(f"⚠️ Column '{column_name}' not found in '{table_name}'. Attempting to add it dynamically.")
                return await self.add_column_to_table(table_name, column_name, column_type, default_value)
            else:
                logging.error(f"❌ Error adding column '{column_name}' to '{table_name}': {e}")
                return False
    async def close(self):
        """
        Closes the database connection pool.
        """
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def disconnect(self):
        """
        Alias for close().
        """
        await self.close()

    async def fetch_new(self, query: str, *args):
        """
        Fetch data (rows) from the database using a provided query.
        """
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)

    async def execute(self, query: str, *args):
        """
        Execute a query in the database that does not necessarily return data.
        """
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                return await connection.execute(query, *args)


    async def fetch(self, query: str):
        """
        Convenience method to fetch all data from the database using a provided query.
        """
        try:
            async with self.pool.acquire() as conn:
                records = await conn.fetch(query)
                return records
        except Exception as e:
            logging.error(e)
            return []

    async def table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the PostgreSQL database.
        """
        query = (
            f"SELECT EXISTS (SELECT FROM information_schema.tables "
            f"WHERE table_name = '{table_name}');"
        )
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(query)
        return exists

    ########################################################################
    # HTTP / Data Fetching Utilities
    ########################################################################

    async def fetch_page(self, url: str) -> dict:
        """
        Fetch a single page of data from a given URL using aiohttp.
        """
        session = await self.get_http_session()
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logging.error(f"Error fetching {url} - {e}")
            return {}

    async def paginate_concurrent(self, url: str, as_dataframe: bool = False, concurrency: int = 25):
        """
        Concurrently paginates through Polygon.io endpoints that contain the "next_url".
        
        :param url: The initial endpoint URL to fetch (with or without apiKey).
        :param as_dataframe: If True, returns a pandas DataFrame.
        :param concurrency: The maximum number of concurrent requests.
        :return: All fetched results (list of dicts or a DataFrame).
        """
        if "apiKey" not in url:
            # Append the apiKey if it's not in the URL
            delimiter = '&' if '?' in url else '?'
            url = f"{url}{delimiter}apiKey={self.api_key}"

        all_results = []
        pages_to_fetch = [url]

        while pages_to_fetch:
            tasks = []
            # Pull up to 'concurrency' URLs to fetch at once
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(self.fetch_page(next_url))

            results = await asyncio.gather(*tasks)
            if results:
                for data in results:
                    if data and isinstance(data, dict):
                        # Some polygon endpoints nest data under 'results'
                        if "results" in data:
                            all_results.extend(data["results"])
                        next_url = data.get("next_url")
                        if next_url:
                            # Ensure the apiKey param is appended
                            delimiter = '&' if '?' in next_url else '?'
                            next_url = f"{next_url}{delimiter}apiKey={self.api_key}"
                            pages_to_fetch.append(next_url)

        if as_dataframe:
            return pd.DataFrame(all_results)
        return all_results

    ########################################################################
    # Database Table Creation & Batch Insert/Upsert
    ########################################################################

    async def create_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_column: str,
        create_history: bool = False
    ):
        """
        Creates a table based on the DataFrame schema if it doesn't exist.
        Optionally creates a _history table with triggers to log changes.
        """
        logging.info(f"Creating table '{table_name}' if not exists...")

        # Map Pandas dtypes to PostgreSQL column types.
        # Removed 'datetime64[ns, US/Eastern]': 'TIMESTAMP WITH TIME ZONE' to avoid tz-aware storage.
        dtype_mapping = {
            'int64': 'INTEGER',
            'float64': 'FLOAT',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime64': 'TIMESTAMP',
            'datetime64[ns]': 'TIMESTAMP',
            'datetime64[ms]': 'TIMESTAMP',
            'datetime64[ns, US/Eastern]': 'TIMESTAMP',  # store as naive local time
            'string': 'TEXT',
            'int32': 'INTEGER',
            'float32': 'FLOAT',
            'datetime64[us]': 'TIMESTAMP',
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
            'bytearray': 'BYTEA',
            'bytes': 'BYTEA',
            'memoryview': 'BYTEA',
            'list': 'INTEGER[]'  # Example for an array of integers
        }

        # Dynamically handle big ints for int64 columns
        for col, col_dtype in zip(df.columns, df.dtypes):
            if col_dtype == 'int64':
                max_val = df[col].max()
                min_val = df[col].min()
                # If values exceed int32 range, switch to BIGINT
                if max_val > 2**31 - 1 or min_val < -2**31:
                    dtype_mapping['int64'] = 'BIGINT'

        history_table_name = f"{table_name}_history"

        async with self.pool.acquire() as connection:
            # Check if table already exists
            table_exists = await connection.fetchval(
                f"SELECT to_regclass('{table_name}')"
            )
            if table_exists is None:
                # Handle unique columns
                if isinstance(unique_column, str):
                    unique_column = [col.strip() for col in unique_column.split(",")]

                unique_constraint = (
                    f'UNIQUE ({", ".join(unique_column)})' if unique_column else ''
                )

                # Build CREATE TABLE query
                create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}'
                               for col, dtype in zip(df.columns, df.dtypes))},
                    "insertion_timestamp" TIMESTAMP,
                    {unique_constraint}
                )
                """
                try:
                    await connection.execute(create_query)
                    logging.info(f"Table {table_name} created successfully.")
                except asyncpg.UniqueViolationError as e:
                    logging.error(f"Unique violation error: {e}")
            else:
                logging.info(f"Table {table_name} already exists.")

            # Optional history table
            if create_history:
                history_create_query = f"""
                CREATE TABLE IF NOT EXISTS {history_table_name} (
                    operation CHAR(1) NOT NULL,
                    changed_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
                    {', '.join(f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}'
                               for col, dtype in zip(df.columns, df.dtypes))}
                );
                """
                await connection.execute(history_create_query)

                trigger_function_query = f"""
                CREATE OR REPLACE FUNCTION save_to_{history_table_name}()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO {history_table_name} (
                        operation, changed_at, {', '.join(f'"{col}"' for col in df.columns)}
                    )
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

                trigger_query = f"""
                DROP TRIGGER IF EXISTS tr_{history_table_name} ON {table_name};
                CREATE TRIGGER tr_{history_table_name}
                AFTER UPDATE OR DELETE ON {table_name}
                FOR EACH ROW EXECUTE FUNCTION save_to_{history_table_name}();
                """
                await connection.execute(trigger_query)
                logging.info(
                    f"History table {history_table_name} and trigger created successfully."
                )

            # Ensure all DataFrame columns exist in the table
            for col, col_dtype in zip(df.columns, df.dtypes):
                alter_query = f"""
                DO $$
                BEGIN
                    BEGIN
                        ALTER TABLE {table_name} ADD COLUMN "{col}" {dtype_mapping.get(str(col_dtype), "TEXT")};
                    EXCEPTION
                        WHEN duplicate_column THEN
                            -- Column already exists, do nothing
                            NULL;
                    END;
                END $$;
                """
                await connection.execute(alter_query)

    def sanitize_value(self, value, col_type):
        """
        Sanitize and format the value for SQL queries based on column type.
        """
        if col_type == 'str':
            return f"'{value}'"
        elif col_type == 'date':
            if isinstance(value, str):
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                    return f"'{value}'"
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
            elif isinstance(value, datetime):
                return f"'{value.strftime('%Y-%m-%d')}'"
        else:
            return str(value)

    async def batch_insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_columns,
        batch_size=250
    ):
        """
        Batch insert (with upsert) a DataFrame into a table. On conflict, update columns.
        """
        try:
            async with lock:
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)
                try:
                    df = df.copy()
                    df['insertion_timestamp'] = pd.to_datetime(
                        [datetime.now() for _ in range(len(df))]
                    )

                    records = df.to_dict(orient='records')

                    async with self.pool.acquire() as connection:
                        column_types = await connection.fetch(
                            f"""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            """
                        )
                        type_mapping = {
                            col['column_name']: col['data_type'] for col in column_types
                        }

                        # Convert unique_columns to a list if it's a string
                        if isinstance(unique_columns, str):
                            unique_columns = [
                                uc.strip() for uc in unique_columns.split(",")
                            ]

                        # Build insert query
                        insert_query = f"""
                        INSERT INTO {table_name} (
                            {', '.join(f'"{col}"' for col in df.columns)}
                        ) 
                        VALUES (
                            {', '.join('$' + str(i) for i in range(1, len(df.columns) + 1))}
                        )
                        ON CONFLICT ({', '.join(unique_columns)})
                        DO UPDATE SET {', '.join(f'"{col}" = excluded."{col}"'
                        for col in df.columns if col not in unique_columns)}
                        """
                        batch_data = []
                        for record in records:
                            new_record = []
                            for col, val in record.items():
                                pg_type = type_mapping.get(col)

                                if pd.isna(val):
                                    new_record.append(None)
                                elif pg_type in [
                                    'timestamp',
                                    'timestamp without time zone',
                                    'timestamp with time zone'
                                ]:
                                    if isinstance(val, np.datetime64):
                                        # Convert numpy datetime64 to Python datetime
                                        new_record.append(
                                            pd.Timestamp(val).to_pydatetime().replace(tzinfo=None)
                                        )
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

                            if len(batch_data) == batch_size:
                                try:
                                    await connection.executemany(insert_query, batch_data)
                                    batch_data.clear()
                                except Exception as e:
                                    logging.error(f"An error occurred while inserting batch: {e}")
                                    await connection.execute('ROLLBACK')

                        # Insert any leftover data
                        if batch_data:
                            try:
                                await connection.executemany(insert_query, batch_data)
                            except Exception as e:
                                logging.error(f"An error occurred while inserting the record: {e}")
                                await connection.execute('ROLLBACK')
                except Exception as e:
                    logging.error(e)
        except Exception as e:
            logging.error(e)

    async def batch_upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_columns,
        batch_size: int = 250
    ):
        """
        Batch upsert a DataFrame into a table, on conflict updates only non-unique columns.
        Storing naive local (Eastern) timestamps in "insertion_timestamp".
        """
        try:
            async with lock:
                # Ensure table exists, create if needed
                if not await self.table_exists(table_name):
                    await self.create_table(df, table_name, unique_columns)

                # Make a copy so we don't modify the original df
                df = df.copy()

                # Generate naive local Eastern timestamps
                eastern = ZoneInfo("America/New_York")
                df['insertion_timestamp'] = [
                    datetime.now(tz=eastern).replace(tzinfo=None) for _ in range(len(df))
                ]

                records = df.to_dict(orient='records')

                if isinstance(unique_columns, str):
                    unique_columns = [uc.strip() for uc in unique_columns.split(",")]

                async with self.pool.acquire() as connection:
                    # Fetch column types from the DB
                    column_types = await connection.fetch(
                        f"""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = '{table_name}'
                        """
                    )
                    type_mapping = {
                        col['column_name']: col['data_type'] for col in column_types
                    }

                    # Prepare INSERT ... ON CONFLICT query
                    insert_query = f"""
                    INSERT INTO {table_name} (
                        {', '.join(f'"{col}"' for col in df.columns)}
                    )
                    VALUES (
                        {', '.join(f'${i}' for i in range(1, len(df.columns) + 1))}
                    )
                    ON CONFLICT ({', '.join(f'"{col}"' for col in unique_columns)})
                    DO UPDATE SET {', '.join(
                        f'"{col}" = EXCLUDED."{col}"'
                        for col in df.columns if col not in unique_columns
                    )}
                    """

                    batch_data = []
                    for record in records:
                        new_record = []
                        for col, val in record.items():
                            pg_type = type_mapping.get(col)
                            if pd.isna(val):
                                new_record.append(None)
                            elif pg_type in [
                                'timestamp', 
                                'timestamp without time zone',
                                'timestamp with time zone'
                            ]:
                                # Force to naive if it might have a tz
                                if isinstance(val, datetime):
                                    # strip tz to avoid “Cannot subtract tz-naive and tz-aware” issues
                                    val = val.replace(tzinfo=None)
                                    new_record.append(val)
                                elif isinstance(val, np.datetime64):
                                    dt_val = pd.Timestamp(val).to_pydatetime()
                                    dt_val = dt_val.replace(tzinfo=None)
                                    new_record.append(dt_val)
                                else:
                                    new_record.append(val)
                            elif pg_type in ['double precision', 'real', 'float']:
                                if not isinstance(val, str):
                                    new_record.append(float(val))
                                else:
                                    new_record.append(val)
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
                                logging.error(f"Error during batch insert: {e}")
                                await connection.execute('ROLLBACK')

                    # Insert remaining batch
                    if batch_data:
                        try:
                            await connection.executemany(insert_query, batch_data)
                        except Exception as e:
                            logging.error(f"Error during final batch insert: {e}")
                            error_msg = str(e).lower()

                            # Specifically handle missing column errors in PostgreSQL
                            if "column" in error_msg and "does not exist" in error_msg:
                                # Extract missing column name explicitly
                                match = re.search(r'column "([^"]+)" does not exist', error_msg)
                                if match:
                                    missing_column = match.group(1)
                                    # Decide column type dynamically, or default to TEXT for safety
                                    await self.add_column_to_table(
                                        table_name=table_name, 
                                        column_name=missing_column, 
                                        column_type="TEXT"  # or dynamically determined
                                    )
                                    logging.info(f"✅ Missing column '{missing_column}' added as TEXT.")
                                else:
                                    logging.error(f"❌ Could not parse missing column name from error: {e}")
                                    await connection.execute('ROLLBACK')


        except Exception as e:
            logging.error(f"An error occurred: {e}")

    async def fetch_all(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a query and return all rows as a list of dictionaries.

        :param query: SQL query string
        :return: List of rows, each as a dictionary
        """
        await self.connect()  # ensures self.conn is open and ready

        if not self.conn:
            raise RuntimeError("Database connection is not established.")

        records = await self.conn.fetch(query)
        return [dict(record) for record in records]
    async def save_to_history(self, df: pd.DataFrame, main_table_name: str, history_table_name: str):
        """
        Saves current data into a _history table for archival.
        """
        try:
            if not await self.table_exists(history_table_name):
                await self.create_table(df, history_table_name, None)
            df['archived_at'] = datetime.now()
            await self.batch_insert_dataframe(df, history_table_name, None)
        except Exception as e:
            logging.error(e)

    ########################################################################
    # Example Data Retrieval and Utility Methods
    ########################################################################

    async def fetch_records(self):
        """
        Example usage to fetch all records from 'opts'.
        """
        await self.connect()
        select_sql = "SELECT * FROM opts"
        async with self.pool.acquire() as conn:
            records = await conn.fetch(select_sql)
            return records

    async def fetch_endpoint(self, endpoint: str, params: dict = None):
        """
        Uses endpoint/parameter combos to fetch from the Polygon API. Automatically
        paginates if 'next_url' is found.
        """
        filtered_params = {k: v for k, v in (params or {}).items() if v is not None}
        if filtered_params:
            query_string = urlencode(filtered_params)
            if query_string:
                # Ensure we append the apiKey
                delimiter = '&' if '?' in endpoint else '?'
                endpoint = f"{endpoint}{delimiter}{query_string}&apiKey={self.api_key}"
        else:
            # If no extra params, ensure the key is appended
            delimiter = '&' if '?' in endpoint else '?'
            endpoint = f"{endpoint}{delimiter}apiKey={self.api_key}"

        data = await self.fetch_page(endpoint)
        # If there's a next_url, do concurrent pagination
        if 'next_url' in data:
            return await self.paginate_concurrent(endpoint, as_dataframe=True, concurrency=40)
        return data

    async def update_options(self, ticker: str):
        """
        Example method to fetch options for a ticker and batch insert them into 'opts' table.
        """
        all_options = await self.find_symbols(ticker)
        df = all_options.df
        await self.batch_insert_dataframe(df, table_name='opts', unique_columns='option_symbol')

    ########################################################################
    # Filtering Methods
    ########################################################################

    async def filter_options(self, select_columns: str = None, **kwargs):
        """
        Filters the 'opts' table based on provided keyword arguments (min/max constraints).
        Returns records from the DB.
        """
        if select_columns is None:
            # Return full columns but default to 'oi > 0'
            query = "SELECT * FROM opts WHERE oi > 0"
        else:
            query = (
                f"SELECT ticker, strike, cp, expiry, {select_columns} FROM opts WHERE oi > 0"
            )

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

        # Build the query with min/max or exact matching
        for key, value in kwargs.items():
            if key in column_types and value is not None:
                column, col_type = column_types[key]
                sanitized_value = self.sanitize_value(value, col_type)

                if 'min' in key:
                    query += f" AND {column} >= {sanitized_value}"
                elif 'max' in key:
                    query += f" AND {column} <= {sanitized_value}"
                else:
                    query += f" AND {column} = {sanitized_value}"

        try:
            async with self.pool.acquire() as conn:
                return await conn.fetch(query)
        except Exception as e:
            logging.error(f"Error during query: {e}")
            return []

    ########################################################################
    # Higher-Level Methods for Option Data
    ########################################################################

    async def get_option_chain_all(
        self,
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
        insert: bool = False
    ):
        """
        Retrieve all option contracts for a specific underlying asset across multiple pages.
        Applies filters like strike price, expiration date, contract type, etc.
        """
        try:
            if not underlying_asset:
                raise ValueError("Underlying asset ticker symbol must be provided.")

            # If underlying_asset starts with "I:", remove that for the snapshot endpoint
            if underlying_asset.startswith("I:"):
                underlying_asset = underlying_asset.replace("I:", "")

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
            # Filter out Nones
            params = {k: v for k, v in params.items() if v is not None}

            endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
            if params:
                query_string = '&'.join(f"{key}={value}" for key, value in params.items())
                endpoint = f"{endpoint}?{query_string}&apiKey={self.api_key}"
            else:
                endpoint = f"{endpoint}?apiKey={self.api_key}"

            response_data = await self.paginate_concurrent(endpoint)

            option_data = UniversalOptionSnapshot(response_data)
            if insert:
                await self.connect()
                await self.batch_insert_dataframe(
                    option_data.df,
                    table_name='all_options',
                    unique_columns='option_symbol'
                )
            return option_data
        except ValueError as ve:
            logging.error(f"ValueError occurred: {ve}")
        except Exception as e:
            logging.error(f"Error in get_option_chain_all: {e}")

    async def find_symbols(
        self,
        underlying_asset: str,
        strike_price=None,
        strike_price_lte=None,
        strike_price_gte=None,
        expiration_date=None,
        expiration_date_gte=None,
        expiration_date_lite=None,
        contract_type=None,
        order=None,
        limit=250,
        sort=None
    ):
        """
        Get all option contracts for an underlying ticker, returning them as a UniversalOptionSnapshot.
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
        params = {k: v for k, v in params.items() if v is not None}

        endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
        if params:
            query_string = '&'.join(f"{k}={v}" for k, v in params.items())
            endpoint += f"?{query_string}&apiKey={self.api_key}"
        else:
            endpoint += f"?apiKey={self.api_key}"

        response_data = await self.paginate_concurrent(endpoint)
        return UniversalOptionSnapshot(response_data)

    async def get_universal_snapshot(self, ticker: str):
        """
        Fetches the universal snapshot for a specific symbol or list of symbols (comma-separated).
        """
        url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=250&apiKey={self.api_key}"
        logging.info(f"Fetching universal snapshot: {url}")

        async with httpx.AsyncClient() as client:
            resp = await client.get(url)
            data = resp.json()
            results = data.get('results')
            if results is not None:
                return UniversalOptionSnapshot(results)
            return None

    async def working_universal(self, symbols_list):
        """
        Fetch universal snapshots for a large list of symbols in chunks to avoid query limit issues.
        """
        async def fetch_chunk(chunk):
            symbols_str = ",".join(chunk)
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={symbols_str}&limit=250&apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.json()

        # Polygon's query limit often ~ 249 symbols per request
        chunk_size = 249
        chunks = list(chunked(symbols_list, chunk_size))
        results = []

        for chunk in chunks:
            data = await fetch_chunk(chunk)
            if "results" in data:
                results.extend(data["results"])
        return WorkingUniversal(results) if results else None

    ########################################################################
    # Example Aggregation / Filter Methods
    ########################################################################



    async def get_price(self, ticker: str):
        """
        Fetch a ticker's snapshot price using Polygon's universal snapshot endpoint.
        Some index tickers are prefixed with 'I:'.
        """
        try:
            if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX']:
                ticker = f"I:{ticker}"
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                if r.status_code == 200:
                    resp_data = r.json()
                    results = resp_data.get('results', [])
                    if results:
                        # 'close' might be nested in results->session->close
                        # Some results might have .get('session').get('close')
                        # We handle that carefully
                        session_data = results[0].get('session')
                        if session_data:
                            return session_data.get('close')
            return None
        except Exception as e:
            logging.error(f"Error fetching price for {ticker}: {e}")
            return None
    async def _get_price_for_ticker(self, ticker: str, client: httpx.AsyncClient) -> Optional[float]:
        """
        Helper method to fetch the latest closing price for a single ticker.
        Returns the close price (float) if available; otherwise, None.
        """
        try:
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            # Uncomment for debugging: print(url)
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get("results")
            if results:
                session = results[0].get("session")
                if session and "close" in session:
                    return session["close"]
            return None
        except Exception as e:
            print(f"Error fetching price for {ticker}: {e}")
            return None

    async def get_price_for_tickers(self, tickers: List[str]) -> AsyncGenerator[Tuple[str, Optional[float]], None]:
        """
        Asynchronously fetch the latest closing price for a list of tickers.
        Uses a single httpx.AsyncClient and asyncio.as_completed to yield each result as soon as it’s ready.
        Yields a tuple (ticker, close_price) for each ticker.
        """
        async with httpx.AsyncClient() as client:
            tasks = []
            for ticker in tickers:
                # Create a task for each ticker.
                task = asyncio.create_task(self._get_price_for_ticker(ticker, client))
                # Attach the ticker to the task.
                tasks.append(task)
            # As tasks complete, yield the ticker and price.
            for completed in asyncio.as_completed(tasks):
                price = await completed
                yield ticker, price


    async def get_symbols(self, ticker: str):
        """
        Fetch current price, then fetch option chains for the next 15 days, 
        and return the list of symbol strings.
        """
        try:
            price = await self.get_price(ticker)
            logging.info(f"Price for {ticker}: {price}")
            options_df = await self.get_option_chain_all(
                underlying_asset=ticker,
                expiration_date_gte=self.today,
                expiration_date_lte=self.fifteen_days_from_now
            )
            logging.info(f"Number of options for {ticker}: {len(options_df.df)}")
            return options_df.option_symbol
        except Exception as e:
            logging.error(e)
            return []

    async def get_trades(self, symbol: str, client):
        """
        Fetch trade data for a specific option symbol and store it in 'option_trades' table.
        """
        try:
            url = f"https://api.polygon.io/v3/trades/{symbol}?limit=50000&apiKey={self.api_key}"
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                if not results:
                    return None

                pattern = r'^O:(?P<ticker>[A-Z]+)(?P<expiry>\d{6})(?P<call_put>[CP])(?P<strike>\d{8})$'
                match_obj = re.match(pattern, symbol)
                if match_obj:
                    ticker = match_obj.group('ticker')
                    expiry_str = match_obj.group('expiry')
                    call_put = match_obj.group('call_put')
                    strike_str = match_obj.group('strike')

                    expiry = datetime.strptime(expiry_str, '%y%m%d').date()
                    strike = int(strike_str) / 1000.0
                else:
                    logging.warning(f"Symbol {symbol} didn't match the pattern.")
                    ticker = ''
                    expiry = None
                    call_put = ''
                    strike = 0.0

                conditions_list = [
                    ', '.join(option_conditions.get(cond, 'Unknown Condition') 
                              for cond in trade.get('conditions', []))
                    for trade in results
                ]
                exchanges = [
                    OPTIONS_EXCHANGES.get(trade.get('exchange'), 'Unknown Exchange')
                    for trade in results
                ]
                ids = [trade.get('id') for trade in results]
                prices = [trade.get('price') for trade in results]
                sequence_numbers = [trade.get('sequence_number') for trade in results]
                sip_timestamps = [convert_to_eastern_time(trade.get('sip_timestamp')) for trade in results]
                sizes = [trade.get('size') for trade in results]
                dollar_costs = [p * s for p, s in zip(prices, sizes)]

                data_dict = {
                    'ticker': [ticker]*len(results),
                    'strike': [strike]*len(results),
                    'call_put': [call_put]*len(results),
                    'expiry': [expiry]*len(results),
                    'option_symbol': [symbol]*len(results),
                    'conditions': conditions_list,
                    'price': prices,
                    'size': sizes,
                    'dollar_cost': dollar_costs,
                    'timestamp': sip_timestamps,
                    'sequence_number': sequence_numbers,
                    'id': ids,
                    'exchange': exchanges
                }
                df = pd.DataFrame(data_dict)

                # Compute various aggregates
                avg_price = df['price'].mean()
                avg_size = df['size'].mean()
                highest_price = df['price'].max()
                lowest_price = df['price'].min()
                highest_size = df['size'].max()

                df['average_price'] = avg_price
                df['average_size'] = avg_size
                df['highest_price'] = highest_price
                df['lowest_price'] = lowest_price
                df['highest_size'] = highest_size

                await self.batch_upsert_dataframe(
                    df,
                    table_name='option_trades',
                    unique_columns=['option_symbol', 'timestamp', 'sequence_number', 
                                    'exchange', 'conditions', 'size', 'price', 'dollar_cost']
                )
                return df
            else:
                logging.error(f"Failed retrieving data for {symbol}. Status code: {response.status_code}")
                return None
        except Exception as e:
            logging.error(e)
            return None

    async def get_trades_for_symbols(self, symbols):
        """
        Fetch trades data for multiple option symbols concurrently and return a combined DataFrame.
        """
        async with httpx.AsyncClient() as client:
            tasks = [self.get_trades(symbol, client) for symbol in symbols]
            results = await asyncio.gather(*tasks)
            dataframes = [df for df in results if df is not None]
            if dataframes:
                return pd.concat(dataframes, ignore_index=True)
            return pd.DataFrame()

    async def fetch_option_aggs(
        self,
        option_symbol: str,
        start_date: str = "2021-01-01",
        timespan: str = 'day'
    ):
        """
        Fetch the historical aggregated data for an option symbol from start_date to present.
        """
        end_date = datetime.today().strftime('%Y-%m-%d')
        url_template = (
            "https://api.polygon.io/v2/aggs/ticker/{}/range/1/{}/{}/{}?adjusted=true&sort=asc&limit=50000&apiKey={}"
        )
        url = url_template.format(option_symbol, timespan, start_date, end_date, self.api_key)

        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'results' in data:
                    df = pd.DataFrame(data['results'])
                    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
                    df.rename(columns={
                        'o': 'open',
                        'c': 'close',
                        'h': 'high',
                        'l': 'low',
                        'v': 'volume'
                    }, inplace=True)
                    return df[['date', 'open', 'close', 'high', 'low', 'volume']]
            logging.error(f"No data available for {option_symbol}. Code: {response.status_code}")
            return pd.DataFrame()

    async def find_lowest_highest_price_df(
        self,
        ticker: str,
        strike: int,
        expiry: str,
        call_put: str
    ) -> pd.DataFrame:
        """
        Given a ticker, strike, expiry, and call/put, find the option symbol, fetch historical data,
        and return DataFrame of lowest/highest prices with their dates.
        """
        await self.connect()
        query = (
            f"SELECT option_symbol FROM master_all_two "
            f"WHERE ticker = '{ticker}' AND strike = {strike} "
            f"AND call_put = '{call_put}' AND expiry = '{expiry}'"
        )
        results = await self.fetch(query)
        df = pd.DataFrame(results, columns=['option_symbol'])
        if df.empty:
            logging.warning("No matching option_symbol found.")
            return pd.DataFrame({"type": [], "date": [], "price": []})

        option_symbol = df['option_symbol'].to_list()[0]
        df_hist = await self.fetch_option_aggs(option_symbol)

        if df_hist.empty:
            return pd.DataFrame({
                "type": ["lowest", "highest"],
                "date": ["N/A", "N/A"],
                "price": [float('inf'), float('-inf')]
            })

        min_row = df_hist.loc[df_hist['low'].idxmin()]
        lowest_price = min_row['low']
        lowest_date = min_row['date'].strftime('%Y-%m-%d')
        max_row = df_hist.loc[df_hist['high'].idxmax()]
        highest_price = max_row['high']
        highest_date = max_row['date'].strftime('%Y-%m-%d')

        return pd.DataFrame({
            "type": ["lowest", "highest"],
            "date": [lowest_date, highest_date],
            "price": [lowest_price, highest_price]
        })

    async def rsi(
        self,
        ticker: str,
        timespan: str,
        limit: str = '1000',
        window: int = 14,
        date_from: str = None,
        date_to: str = None
    ):
        """
        Fetch RSI data for a ticker from the Polygon.io indicators endpoint.
        """
        if date_from is None:
            date_from = self.eight_days_ago
        if date_to is None:
            date_to = self.today

        endpoint = (
            f"https://api.polygon.io/v1/indicators/rsi/{ticker}?"
            f"timespan={timespan}&timestamp.gte={date_from}&timestamp.lte={date_to}&"
            f"limit={limit}&window={window}&apiKey={self.api_key}"
        )

        session = await self.get_http_session()
        try:
            async with session.get(endpoint) as resp:
                datas = await resp.json()
                if datas is not None:
                    return RSI(datas, ticker)
        except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ContentTypeError) as e:
            logging.error(f"RSI error for {ticker} - {e}")

    async def rsi_snapshot(self, ticker: str):
        """
        Fetch RSI values for multiple timespans and build a DataFrame of the results.
        """
        try:
            components = get_human_readable_string(ticker)
            symbol = components.get('underlying_symbol')
            strike = components.get('strike_price')
            expiry = components.get('expiry_date')
            call_put = str(components.get('call_put')).lower()

            timespans = ['minute', 'hour', 'day', 'week', 'month']
            all_data_dicts = []

            for ts in timespans:
                rsi_data = await self.rsi(ticker, ts, limit='1')
                if rsi_data and rsi_data.rsi_value and len(rsi_data.rsi_value) > 0:
                    rsi_val = rsi_data.rsi_value[0]
                else:
                    rsi_val = 0

                all_data_dicts.append({
                    'timespan': ts,
                    'option_symbol': ticker,
                    'ticker': symbol,
                    'strike': strike,
                    'expiry': expiry,
                    'call_put': call_put,
                    'rsi': rsi_val
                })
            return pd.DataFrame(all_data_dicts)
        except Exception as e:
            logging.error(e)
            return pd.DataFrame()

    async def option_aggregates(
        self,
        ticker: str,
        timespan: str = 'second',
        date_from: str = '2019-09-17',
        date_to: str = None,
        limit: int = 50000,
        as_dataframe: bool = False
    ):
        """
        Gets all aggregates for a ticker or option symbol from date_from to date_to.
        """
        if date_to is None:
            date_to = self.today

        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}/"
            f"{date_from}/{date_to}?adjusted=true&sort=desc&limit={limit}&apiKey={self.api_key}"
        )

        results = await self.paginate_concurrent(url, as_dataframe=False, concurrency=50)
        if as_dataframe:
            return pd.DataFrame(results)
        return results

    ########################################################################
    # Additional Option Data Methods
    ########################################################################

    async def gpt_filter(self, **kwargs):
        """
        Returns table chunks of filtered options from 'opts'.
        Example usage: 
            await polygon_client.gpt_filter(ticker='AAPL', strike_min=100, strike_max=120)
        """
        await self.connect()
        records = await self.filter_options(**kwargs)
        df = pd.DataFrame(records)
        if df.empty:
            return ["No records found."]

        table = tabulate(df, headers=df.columns, tablefmt='fancy_grid', showindex=False)
        # Break apart data into chunks of 4000 characters
        chunks = chunk_string(table, 4000)
        return chunks

    ########################################################################
    # Example of a Complex Theoretical Pricing (Binomial Model)
    ########################################################################

    def binomial_american_option(self, S, K, T, r, sigma, N, option_type='put'):
        """
        Binomial model for American option pricing.
        """
        dt = T / N
        u = np.exp(sigma * np.sqrt(dt))
        d = 1 / u
        p = (np.exp(r * dt) - d) / (u - d)

        # Initialize the stock tree
        ST = np.zeros((N + 1, N + 1))
        ST[0, 0] = S

        for i in range(1, N + 1):
            ST[i, 0] = ST[i - 1, 0] * u
            for j in range(1, i + 1):
                ST[i, j] = ST[i - 1, j - 1] * d

        # Option payoff at maturity
        option = np.zeros((N + 1, N + 1))
        for j in range(N + 1):
            if option_type == 'call':
                option[N, j] = max(0, ST[N, j] - K)
            else:
                option[N, j] = max(0, K - ST[N, j])

        # Roll back the binomial tree
        for i in range(N - 1, -1, -1):
            for j in range(i + 1):
                exercise_value = (
                    max(ST[i, j] - K, 0) if option_type == 'call'
                    else max(K - ST[i, j], 0)
                )
                hold_value = np.exp(-r * dt) * (
                    p * option[i + 1, j] + (1 - p) * option[i + 1, j + 1]
                )
                option[i, j] = max(exercise_value, hold_value)

        return option[0, 0]

    async def get_theoretical_price(self, ticker: str):
        """
        Example skeleton for calculating theoretical option prices using a binomial model.
        (Requires real-time or historical data, placeholders below.)
        """
        # In your original code, you mentioned 'self.yf.fast_info(ticker)', which doesn't exist here.
        # This is a placeholder for demonstration:
        current_price = 150.0  # Replace with a real fetch

        all_options_data = await self.get_option_chain_all(underlying_asset=ticker)
        risk_free_rate = 0.0565  # e.g., 5.65%
        N = 100  # Number of steps
        theoretical_values = []

        # Placeholder loops: you'd adapt the actual columns from your universal snapshot
        for idx, row in all_options_data.df.iterrows():
            bid = row.get('bid')
            ask = row.get('ask')
            iv = row.get('implied_volatility')
            strike = row.get('strike')
            expiry = row.get('expiry')
            option_type = row.get('contract_type')
            dte = row.get('days_to_expiry', 30)  # placeholder
            # Convert days to fraction of year
            T = float(dte) / 365.0

            if iv is None:
                continue
            sigma = iv

            theoretical_price = self.binomial_american_option(
                S=current_price, K=strike, T=T, r=risk_free_rate, sigma=sigma,
                N=N, option_type=option_type
            )

            theoretical_values.append({
                'ticker': ticker,
                'current_price': current_price,
                'iv': iv,
                'strike': strike,
                'expiry': expiry,
                'bid': bid,
                'theoretical_price': theoretical_price,
                'ask': ask,
                'type': option_type
            })

        return theoretical_values

    ########################################################################
    # Example Accessor Methods (Table Columns, etc.)
    ########################################################################

    async def get_table_columns(self, table: str) -> list:
        """
        Fetches the column names of a given table dynamically and returns them as a list.
        """
        try:
            # You can also just use self.pool if connected
            conn = await asyncpg.connect(self.connection_string)
            query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position;
            """
            rows = await conn.fetch(query, table)
            await conn.close()
            return [row['column_name'] for row in rows]
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return []


    async def stats_by_expiry(self, ticker: str):
        """CHECKS CALL VOLUME/OI VS PUT VOLUME/OI PER EXPIRY AND DETERMINES OI AND VOLUME WINNERS"""

        # Retrieve the option chain as a DataFrame.
        opts = await self.get_option_chain_all(ticker)

        # Filter DataFrame for calls and puts using dot notation.
        call_opts = opts[opts['call_put'] == 'call']
        put_opts  = opts[opts['call_put'] == 'put']

        # Group by expiry and aggregate volume and open interest (oi) for calls.
        call_summary = call_opts.groupby('expiry').agg(
            volume=('volume', 'sum'),
            oi=('oi', 'sum')
        )

        # Group by expiry and aggregate volume and open interest (oi) for puts.
        put_summary = put_opts.groupby('expiry').agg(
            volume=('volume', 'sum'),
            oi=('oi', 'sum')
        )

        # Join the call and put summaries by expiry.
        summary = call_summary.join(put_summary, lsuffix='_call', rsuffix='_put').fillna(0)

        # Determine the "oi winner" per expiry:
        summary['oi_winner'] = summary.apply(
            lambda row: 'call' if row.oi_call > row.oi_put 
                        else ('put' if row.oi_call < row.oi_put else 'tie'),
            axis=1
        )

        # Determine the "volume winner" per expiry:
        summary['volume_winner'] = summary.apply(
            lambda row: 'call' if row.volume_call > row.volume_put 
                        else ('put' if row.volume_call < row.volume_put else 'tie'),
            axis=1
        )

        return summary


    async def get_table_info(self, table_name):
        """Fetch column names and descriptions for a given PostgreSQL table."""

        await self.connect()  # Ensure connection to the database

        query = f"""
            SELECT 
                a.attname AS column_name, 
                COALESCE(d.description, 'No description found') AS description
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_description d 
                ON a.attrelid = d.objoid 
                AND a.attnum = d.objsubid
            WHERE c.relname = '{table_name}' 
            AND a.attnum > 0 
            AND NOT a.attisdropped
            ORDER BY a.attnum;
        """

        results = await self.fetch(query)

        # Debugging output to check if descriptions are retrieved
        print(results)

        # Convert fetched data to DataFrame
        df = pd.DataFrame(results, columns=['column_name', 'description'])

        return df