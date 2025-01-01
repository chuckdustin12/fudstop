
import json
# Add the project directory to the sys.path
from asyncpg import create_pool
import asyncpg
import pandas as pd
from asyncio import Lock
import numpy as np
from datetime import datetime

def format_large_number(number):
    """
    Formats a number into a human-readable format (e.g., 1K, 1M, 1B, etc.) including negative numbers.
    Handles NoneType to avoid TypeError.
    """
    if number is None:
        return 0.0  # or any placeholder you prefer for None values
    prefix = "-" if number < 0 else ""
    abs_number = abs(number)
    
    if abs_number < 1000:
        return f"{prefix}{abs_number}"
    elif abs_number < 1000000:
        return f"{prefix}{abs_number/1000:.1f}K"
    elif abs_number < 1000000000:
        return f"{prefix}{abs_number/1000000:.1f}M"
    else:
        return f"{prefix}{abs_number/1000000000:.1f}B"
def lowercase_columns(df):
    # Rename columns to lowercase after converting them to strings
    df.columns = map(lambda x: str(x).lower(), df.columns)
    return df

def format_large_numbers_in_dataframe(df):
    """
    Dynamically formats all numeric columns in a DataFrame to readable large numbers.
    """
    formatted_df = df.copy()
    numeric_columns = formatted_df.select_dtypes(include=['number']).columns

    for column in numeric_columns:
        formatted_df[column] = formatted_df[column].apply(format_large_number)
    
    return formatted_df
class AsyncpgSDK:
    def __init__(self, host:str='localhost', user:str='chuck', password:str='fud', database:str='markets', port:int=5432):
        """
        Database SDK for ASYNCPG

        Pass in your credentials.

        host

        user

        password

        database

        port
        """
        self.pool = None
        self.host=host
        self.user=user
        self.password=password
        self.port=port
        self.database=database
        self.lock = Lock()
  

    async def connect(self):
        """Establishes a pooled connection."""
        self.pool = await create_pool(
            host=self.host, user=self.user, database=self.database, port=5432, password=self.password, min_size=1, max_size=40
        )

        return self.pool

    

    async def create_table(self, df, table_name, unique_column:str='insertion_timestamp'):
        """Auto creates tables based on the makeup of the dataframe.
        
        >>> table_name: the name of the table
        >>> unique_column: the unique constraint to set (defauls to insertion_timestamp)
        """


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





    async def fetch_all_tables(self):
        """
        Fetches the names of all tables in a specified schema of the PostgreSQL database.

        Returns:
        - List[str]: A list of table names from the 'public' schema.
        """
        # Connect to the PostgreSQL database
        conn = await self.connect
        
        # SQL query to select the table names from the specified schema
        query = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        
        # Execute the query
        rows = await conn.fetch(query)
        
        # Extract table names from the rows
        table_names = [row['table_name'] for row in rows]
        
        return table_names

    async def fetch_table_data(self, table_name: str):
        # Validate the table_name against a known list of table names
        
        # Connect to the PostgreSQL database
        await self.connect()
        
        # Initialize the base query
        query = f"SELECT * FROM {table_name}"
        
        # Initialize a list to hold query conditions
        conditions = []
        
        # Add conditions to the query if any
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
 
        
        # Execute the query
        rows = await self.fetch(query)
        
        if rows:
            # Extract column names dynamically from the first row
            columns = list(rows[0].keys())
            
            # Initialize a dictionary with column names as keys
            data_dict = {col: [] for col in columns}
            
            # Populate the dictionary with data
            for row in rows:
                for col in columns:
                    data_dict[col].append(row[col])
            
            # Create a DataFrame using the dynamic columns
            df = pd.DataFrame(data_dict, columns=columns)
            return df

        return None  # Return None if no data is fetched
        


    async def batch_insert_dataframe(self, df, table_name, unique_columns, batch_size=250):
        """Batch inserts data into the created table.
        
        >>> table_name: the name of the table
        >>> unique_columns: the unique constraint (defaults to insertion_timestamp)
        >>> batch_size: the batch size to set for insertion
        """


        async with self.lock:
            if not await self.table_exists(table_name):
                await self.create_table(df, table_name, unique_columns)

            # Debug: Print DataFrame columns before modifications
            #print("Initial DataFrame columns:", df.columns.tolist())
            
            df = df.copy()
            
            df['insertion_timestamp'] = pd.to_datetime([datetime.now() for _ in range(len(df))])


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
                    
                        batch_data.append(new_record)

                        if len(batch_data) == batch_size:
                            try:
                                
                             
                                await connection.executemany(insert_query, batch_data)
                                batch_data.clear()
                            except Exception as e:
                                print(f"An error occurred while inserting the record: {e}")
                                await connection.execute('ROLLBACK')
                    
                    if batch_data:
                        # Convert all elements in batch_data to strings
                        #batch_data = [[str(value) if isinstance(value, float) else value for value in row] for row in batch_data]
                        
                        try:
                            await connection.executemany(insert_query, batch_data)
                        except Exception as e:
                            print(f"An error occurred while inserting the record: {e}")
                            await connection.execute('ROLLBACK')


    async def save_to_history(self, df, main_table_name, history_table_name):
        # Assume the DataFrame `df` contains the records to be archived
        if not await self.table_exists(history_table_name):
            await self.create_table(df, history_table_name, None)

        df['archived_at'] = datetime.now()  # Add an 'archived_at' timestamp
        await self.batch_insert_dataframe(df, history_table_name, None)


        
    async def table_exists(self, table_name):
        """Check if a table exists before batch insertion
        
        
        (auto function - no need to call this independently as it is managed by the create_table function)
        
        """
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
  
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(query)
        return exists
    



    async def fetch(self, query):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return records
        

    async def fetch_data(self, table_name: str, columns: list = None, where_clause: str = None, 
                               order_by: str = None, limit: int = None):
        # Connect to the PostgreSQL database
        await self.connect()

        # Validate the table_name and columns against known table names and columns
        # This could be done with introspection or by checking against a predefined list
        # For simplicity, this step is skipped, but it can be added if needed.

        # Create the SQL query dynamically
        if columns is None:
            columns = ["*"]  # Select all columns if none are specified
        columns_str = ", ".join(columns)
        
        query = f"SELECT {columns_str} FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"

        # Execute the query
        records = await self.fetch(query)

        # Extract column names from the query result
        if columns == ["*"]:
            # If all columns were selected, extract column names from the records
            column_names = [key for key in records[0].keys()]
        else:
            # Use the provided column names
            column_names = columns

        # Convert the records to a pandas DataFrame
        df = pd.DataFrame(records, columns=column_names)

        return df
    

    async def close(self):
        await self.close()
