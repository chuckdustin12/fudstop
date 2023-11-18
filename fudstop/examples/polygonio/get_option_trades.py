
import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()


from apis.helpers import get_human_readable_string, map_conditions, OPTIONS_EXCHANGES

db_config = {
    "host": os.environ.get('DB_HOST', 'localhost'),
    "port": int(os.environ.get('DB_PORT', 5432)),
    "user": os.environ.get('DB_USER', 'postgres'),
    "password": os.environ.get('DB_PASSWORD', 'fud'),
    "database": os.environ.get('DB_NAME', 'polygon')
}


import asyncio
import pandas as pd
from apis.polygonio.polygon_options import PolygonOptions


opts = PolygonOptions(**db_config)
from asyncio import Semaphore

sema = Semaphore(15)


async def process_trades(ticker):
    async with sema:
        trades = await opts.option_trades(ticker)
        components = get_human_readable_string(trades)
        symbol = components.get('underlying_symbol')
        strike = components.get('strike_price')
        expiry = components.get('expiry_date')
        call_put = components.get('call_put')
            
        # Assuming trades is a DataFrame and sip_timestamp is a column with Unix timestamps
        # First, convert the Unix timestamps to datetime objects, normalize to UTC, and then convert to US/Eastern time
        # Convert the Unix timestamps to datetime objects (without timezone)
        trades['sip_timestamp'] = pd.to_datetime(trades['sip_timestamp'], unit='ns')

        # Apply timezone localization and conversion for each timestamp individually
        trades['sip_timestamp'] = trades['sip_timestamp'].apply(lambda x: x.tz_localize('UTC').tz_convert('US/Eastern'))

        # Format these datetime objects to a string format
        trades['sip_timestamp'] = trades['sip_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        trades['conditions'] = trades['conditions'].apply(map_conditions)
        trades['conditions'] = trades['conditions'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        if 'participant_timestamp' in trades.columns:
            trades = trades.drop(columns=['participant_timestamp'])

        if 'exchange' in trades.columns:
            trades = trades.drop(columns=['exchange'])

        if 'sequence_number' in trades.columns:
            trades = trades.drop(columns=['sequence_number'])

        if 'id' in trades.columns:
            trades = trades.drop(columns=['id'])
        trades['option_symbol'] = ticker
        trades['ticker'] = symbol
        trades['strike'] = strike
        trades['expiry'] = expiry
        expiry = pd.to_datetime(trades['expiry']).dt.strftime('%Y-%m-%d')
        trades['expiry'] = expiry
        trades['call_put'] = call_put
        print(trades.columns)
        


        asyncio.create_task(opts.batch_insert_dataframe(trades, 'option_trades', unique_columns='ticker, strike, expiry, call_put'))


async def process_batch(tickers):
    for ticker in tickers:
        await process_trades(ticker)


async def get_all_trades():
        await opts.connect()
    
    
        tickers = await opts.get_tickers()

        batch_size = 250  # Process 250 tickers at a time
        tasks = []
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            # Create an asynchronous task for each batch
            task = asyncio.create_task(process_batch(batch))
            tasks.append(task)

        # Run all batch processing tasks concurrently
        await asyncio.gather(*tasks)


asyncio.run(get_all_trades())