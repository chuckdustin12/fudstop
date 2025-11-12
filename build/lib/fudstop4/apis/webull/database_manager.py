import os
import asyncpg
from asyncio import Lock
import pandas as pd
from datetime import datetime
import numpy as np
from dateutil.parser import parse

lock = Lock()

class DatabaseManager:
    def __init__(self, host, port, user, database, password):
        self.conn = None
        self.pool = None
        self.host=host
        self.port=port
        self.user=user
        self.database=database
        self.password=password
        self.chat_memory = []  # In-memory list to store chat messages

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=self.host,user=self.user,password=self.password,port=self.port,database=self.database, min_size=1, max_size=75
        )

        return self.pool
    
    async def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');"
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                exists = await conn.fetchval(query)
        return exists
    

    async def create_table(self, df, table_name):
        print("Connected to the database.")
        dtype_mapping = {
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'object': 'TEXT',
            'bool': 'BOOLEAN',
            'datetime.date': 'TIMESTAMP',
            'datetime.datetime': 'TIMESTAMP',
            'datetime64[ns]': 'timestamp',
            'datetime64[ms]': 'timestamp',
            'datetime64[ns, US/Eastern]': 'TIMESTAMP WITH TIME ZONE',
            'string': 'TEXT'
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
                create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(f'"{col}" {dtype_mapping[str(dtype)]}' for col, dtype in zip(df.columns, df.dtypes))}
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
        async with lock:
            if not await self.table_exists(table_name):
                await self.create_table(df, table_name, unique_columns)

            # Debug: Print DataFrame columns before modifications
            #print("Initial DataFrame columns:", df.columns.tolist())
            
            df = df.copy()
            df.dropna(inplace=True)
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
    async def insert_webull_option(self, option_data):
            async with self.pool.acquire() as conn:
                
                await conn.execute("""
                    INSERT INTO wb_opts (
                        open, high, low, close, change, change_ratio, strike, expiry,
                        call_put, oi, oi_change, active_level, cycle, volume,
                        latest_price_vol, delta, imp_vol, gamma, theta, vega, rho, option_symbol, underlying_symbol,trade_time, tickerid
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 
                        $17, $18, $19, $20, $21, $22, $23, $24,$25
                    )
                    ON CONFLICT (option_symbol, underlying_symbol, strike, expiry, call_put)
                    DO UPDATE SET
                    open = EXCLUDED.open, 
                    high = EXCLUDED.high, 
                    low = EXCLUDED.low, 
                    close = EXCLUDED.close, 
                    change = EXCLUDED.change, 
                    change_ratio = EXCLUDED.change_ratio, 
                    oi = EXCLUDED.oi, 
                    oi_change = EXCLUDED.oi_change, 
                    active_level = EXCLUDED.active_level, 
                    cycle = EXCLUDED.cycle, 
                    volume = EXCLUDED.volume, 
                    latest_price_vol = EXCLUDED.latest_price_vol, 
                    delta = EXCLUDED.delta, 
                    imp_vol = EXCLUDED.imp_vol, 
                    gamma = EXCLUDED.gamma, 
                    theta = EXCLUDED.theta, 
                    vega = EXCLUDED.vega, 
                    rho = EXCLUDED.rho, 
                    trade_time = EXCLUDED.trade_time
                    """, 

                option_data['open'], option_data['high'], option_data['low'], option_data['close'],
                option_data['change'], option_data['change_ratio'], option_data['strike'], option_data['expiry'],
                option_data['call_put'], option_data['oi'], option_data['oi_change'], option_data['active_level'],
                option_data['cycle'], option_data['volume'], option_data['latest_price_vol'], option_data['delta'],
                option_data['imp_vol'], option_data['gamma'], option_data['theta'], option_data['vega'], 
                option_data['rho'], option_data['option_symbol'], option_data['underlying_symbol'],option_data['trade_time'], option_data['tickerid']
                )
    async def disconnect(self):
        await self.pool.close()

    async def fetch(self, query):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return records