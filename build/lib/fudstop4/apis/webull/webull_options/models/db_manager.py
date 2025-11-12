import asyncpg
from datetime import datetime, date
from asyncio import Lock
import numpy as np
import pandas as pd
lock = Lock()
import uuid
from pytz import timezone
from asyncpg.exceptions import UniqueViolationError
class DBManager:
    def __init__(self, host='localhost', user='chuck', password='fud', database='fudstop3', port=5432):
        self.db_params = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port
        }
        self.pool = None
    async def fetch(self, query):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return records
    async def create_pool(self, min_size=10, max_size=50):
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    min_size=min_size, 
                    max_size=max_size, 
                    **self.db_params
                )
            except Exception as e:
                print(f"Error creating connection pool: {e}")
                raise

    async def close_pool(self):
        if self.pool:
            await self.pool.close()

    async def get_connection(self):
        if self.pool is None:
            await self.create_pool()

        return await self.pool.acquire()

    async def release_connection(self, connection):
        await self.pool.release(connection)



    async def insert_volume_analysis(self, option_id, option_symbol, underlying_symbol, strike_price, expiry, call_put, buy, sell, price, ratio, date):
        insert_query = '''
            INSERT INTO volume_analysis (option_id, option_symbol, underlying_symbol, strike_price, 
                                         expiry, call_put, buy, sell, price, ratio, date)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (option_id, date) DO NOTHING;
        '''

        connection = await self.get_connection()  # Ensure connection is established

        try:
            # Insert a single record
            await connection.execute(insert_query, option_id, option_symbol, underlying_symbol, strike_price, expiry, call_put, buy, sell, price, ratio, date)
        except Exception as e:
            print(f"Error during insert: {e}")
        finally:
            await connection.close()
    async def batch_insert_option_data(self, ticker_ids, belong_ticker_ids, opens, highs, lows, 
                                    strike_prices, pre_closes, open_interests, volumes, latest_price_vols, 
                                    deltas, vegas, imp_vols, gammas, thetas, rhos, closes, changes, 
                                    change_ratios, expire_dates, open_int_changes, active_levels, cycles, 
                                    call_puts, option_symbols, underlying_symbols):
        if not ticker_ids:
            return

        connection = await self.get_connection()
        try:


            # Create table and history table if they don't exist
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS wb_opts (
                    ticker_id INTEGER, belong_ticker_id INTEGER, open FLOAT, high FLOAT, low FLOAT, 
                    strike_price INTEGER, pre_close FLOAT, open_interest FLOAT, volume FLOAT, 
                    latest_price_vol FLOAT, delta FLOAT, vega FLOAT, imp_vol FLOAT, gamma FLOAT, 
                    theta FLOAT, rho FLOAT, close FLOAT, change FLOAT, change_ratio FLOAT, 
                    expire_date DATE, open_int_change FLOAT, active_level FLOAT, 
                    cycle FLOAT, call_put VARCHAR, option_symbol VARCHAR PRIMARY KEY, 
                    underlying_symbol VARCHAR
                );
                CREATE TABLE IF NOT EXISTS wb_opts_history (
                    id SERIAL PRIMARY KEY, ticker_id INTEGER, belong_ticker_id INTEGER, open FLOAT, 
                    high FLOAT, low FLOAT, strike_price INTEGER, pre_close FLOAT, open_interest FLOAT, 
                    volume FLOAT, latest_price_vol FLOAT, delta FLOAT, vega FLOAT, imp_vol FLOAT, 
                    gamma FLOAT, theta FLOAT, rho FLOAT, close FLOAT, change FLOAT, 
                    change_ratio FLOAT, expire_date DATE, open_int_change FLOAT, 
                    active_level FLOAT, cycle FLOAT, call_put VARCHAR, option_symbol VARCHAR, 
                    underlying_symbol VARCHAR, archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            await connection.execute(create_table_query)

            # Archive existing records to history table
            try:
                archive_query = f"""
                    INSERT INTO wb_opts_history (ticker_id, belong_ticker_id, open, high, low, strike_price, 
                                                    pre_close, open_interest, volume, latest_price_vol, delta, 
                                                    vega, imp_vol, gamma, theta, rho, close, change, 
                                                    change_ratio, expire_date, open_int_change, active_level, 
                                                    cycle, call_put, option_symbol, underlying_symbol)
                    SELECT ticker_id, belong_ticker_id, open, high, low, strike_price, 
                        pre_close, open_interest, volume, latest_price_vol, delta, 
                        vega, imp_vol, gamma, theta, rho, close, change, 
                        change_ratio, expire_date, open_int_change, active_level, 
                        cycle, call_put, option_symbol, underlying_symbol
                    FROM wb_opts
                    WHERE option_symbol = ANY($1);
                """
                await connection.execute(archive_query, ([symbol for symbol in option_symbols],))
            except UniqueViolationError:
                print(f'Exists - Skipping - No change')

            # Insert or update new records
            insert_query = f"""
                INSERT INTO wb_opts (
                    ticker_id, belong_ticker_id, open, high, low, strike_price, 
                    pre_close, open_interest, volume, latest_price_vol, delta, 
                    vega, imp_vol, gamma, theta, rho, close, change, 
                    change_ratio, expire_date, open_int_change, active_level, 
                    cycle, call_put, option_symbol, underlying_symbol
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
                )
                ON CONFLICT (option_symbol) DO UPDATE SET
                    ticker_id = EXCLUDED.ticker_id,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    strike_price = EXCLUDED.strike_price,
                    pre_close = EXCLUDED.pre_close,
                    open_interest = EXCLUDED.open_interest,
                    volume = EXCLUDED.volume,
                    latest_price_vol = EXCLUDED.latest_price_vol,
                    delta = EXCLUDED.delta,
                    vega = EXCLUDED.vega,
                    imp_vol = EXCLUDED.imp_vol,
                    gamma = EXCLUDED.gamma,
                    theta = EXCLUDED.theta,
                    rho = EXCLUDED.rho,
                    close = EXCLUDED.close,
                    change = EXCLUDED.change,
                    change_ratio = EXCLUDED.change_ratio,
                    open_int_change = EXCLUDED.open_int_change,
                    active_level = EXCLUDED.active_level,
                    cycle = EXCLUDED.cycle,
                    call_put = EXCLUDED.call_put,
                    underlying_symbol = EXCLUDED.underlying_symbol;
            """

            values = list(zip(
                ticker_ids, belong_ticker_ids, opens, highs, lows, 
                strike_prices, pre_closes, open_interests, volumes, latest_price_vols, 
                deltas, vegas, imp_vols, gammas, thetas, rhos, closes, changes, 
                change_ratios, expire_dates, open_int_changes, active_levels, cycles, 
                call_puts, option_symbols, underlying_symbols
            ))

            await connection.executemany(insert_query, values)
        except Exception as e:
            print(f"Error in batch insert for opts: {e}")
        finally:
            await self.release_connection(connection)

    async def query_derivative_ids_in_price_range(self, symbol, current_price, threshold_percentage=5, strike=None, expiry=None):
        table_name = f"{symbol}"
        connection = await self.get_connection()
        try:
            # Calculate price range
            upper_bound = current_price * (1 + threshold_percentage / 100)
            lower_bound = current_price * (1 - threshold_percentage / 100)

            # Base query
            query = f"""
                SELECT ticker_id, option_symbol FROM wb_opts
                WHERE strike_price BETWEEN $1 AND $2
            """
            
            # List to hold parameters
            params = [lower_bound, upper_bound]

            # Adding conditions for strike and expiry if they are provided
            if strike is not None:
                params.append(strike)
                query += f" AND strike_price = ${len(params)}"
            if expiry is not None:
                params.append(expiry)
                query += f" AND expire_date = ${len(params)}"

            # Fetch derivative IDs and option symbols
            results = await connection.fetch(query, *params)

            return [(row['ticker_id'], row['option_symbol']) for row in results]
        except Exception as e:
            print(f"Error querying derivative IDs in price range: {e}")
            return []
        finally:
            await self.release_connection(connection)

    async def insert_base_data(self, base_data):
        conn = await self.get_connection()

        # SQL to move existing data to history table
        move_to_history_query = '''
            INSERT INTO stock_data_history 
            SELECT *, CURRENT_TIMESTAMP FROM stock_data WHERE ticker_id = $1
        '''

        # SQL to update existing data
        update_query = '''
            UPDATE stock_data SET
                symbol = $2, underlying_volatility = $3, underlying_change = $4,
                underlying_change_percent = $5, underlying_volume = $6, underlying_close = $7,
                underlying_preclose = $8, underlying_high = $9, underlying_low = $10,
                underlying_open = $11, name = $12
            WHERE ticker_id = $1
        '''

        # SQL to insert new data
        insert_query = '''
            INSERT INTO stock_data (
                ticker_id, symbol, underlying_volatility, underlying_change, 
                underlying_change_percent, underlying_volume, underlying_close, 
                underlying_preclose, underlying_high, underlying_low, 
                underlying_open, name
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (ticker_id) DO NOTHING
        '''

        # Execute the queries
        await conn.execute(move_to_history_query, base_data.tickerId)
        await conn.execute(update_query,
                        base_data.tickerId, base_data.disSymbol, base_data.vol1y,
                        base_data.change, base_data.changeRatio, base_data.volume,
                        base_data.close, base_data.preClose, base_data.high,
                        base_data.low, base_data.open, base_data.name)
        await conn.execute(insert_query,
                        base_data.tickerId, base_data.disSymbol, base_data.vol1y,
                        base_data.change, base_data.changeRatio, base_data.volume,
                        base_data.close, base_data.preClose, base_data.high,
                        base_data.low, base_data.open, base_data.name)

        await conn.close()

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
            df['insertion_timestamp'] = [datetime.now() for _ in range(len(df))]  # Adjust format here if needed

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
                    # Use datetime objects directly
                    df['insertion_timestamp'] = [datetime.now() for _ in range(len(df))]
                    try:
                        await connection.executemany(insert_query, batch_data)
                    except Exception as e:
                        print(f"An error occurred while inserting the record: {e}")
                        await connection.execute('ROLLBACK')
                        raise
    def dataframe_to_tuples(self, df):
        """
        Converts a Pandas DataFrame to a list of tuples, each tuple representing a row.
        """
        return [tuple(x) for x in df.to_numpy()]
    async def batch_insert_wb_dataframe(self, df, table_name, history_table_name):
        """
        Batch inserts a DataFrame into the specified table with history handling.

        :param df: Pandas DataFrame to be inserted.
        :param table_name: Name of the table where data will be inserted.
        :param history_table_name: Name of the history table for storing changes.
        :param unique_column: Column name used to identify unique records.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for record in self.dataframe_to_tuples(df):
                    # Check for existing record


                    # Insert or update record
                    columns = ', '.join(df.columns)
                    upsert_query = f"""
                    INSERT INTO {table_name} ({columns}) 
                    VALUES ({', '.join([f'${i+1}' for i in range(len(record))])})
                    ON CONFLICT (option_symbol) 
                    DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns if col != 'option_symbol'])}
                    """
                    await conn.execute(upsert_query, *record)

    async def upsert_with_history(self, df, table_name, history_table_name, unique_column):
        """
        Upserts data from DataFrame, moving changed data to a history table.

        :param df: DataFrame with data to insert.
        :param table_name: Name of the main table.
        :param history_table_name: Name of the history table.
        :param unique_column: Column name used to identify unique records.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for record in self.dataframe_to_tuples(df):
                    # Check if the record exists and if it has changed
                    existing_record = await conn.fetchrow(
                        f"SELECT * FROM {table_name} WHERE {unique_column} = $1", record[0]
                    )
                    if existing_record and existing_record != record:
                        # Move changed data to history table
                        columns = ', '.join(df.columns)
                        history_columns = columns + ', history_timestamp'
                        history_values = ', '.join([f'${i+1}' for i in range(len(record))]) + ', NOW()'
                        await conn.execute(
                            f"INSERT INTO {history_table_name} ({history_columns}) VALUES ({history_values})",
                            *record
                        )

                    # Insert or update new record
                    upsert_query = f"""
                    INSERT INTO {table_name} ({columns}) 
                    VALUES ({', '.join([f'${i+1}' for i in range(len(record))])})
                    ON CONFLICT ({unique_column}) 
                    DO UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in df.columns if col != unique_column])}
                    """
                    await conn.execute(upsert_query, *record)
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


    async def get_ticker_id_by_symbol(self, option_symbol, table_name:str='webull_options'):
        async with self.pool.acquire() as connection:
            query = f"SELECT ticker_id FROM {table_name} WHERE option_symbol = $1"
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