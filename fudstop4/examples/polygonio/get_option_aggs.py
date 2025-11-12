import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import asyncio

from apis.polygonio.polygon_options import PolygonOptions
from apis.helpers import convert_datetime_list, get_human_readable_string
opts = PolygonOptions(database='fudstop3')



async def process_ticker(opts, ticker):
    try:
        print(ticker)
        components = get_human_readable_string(ticker)
        underlying_symbol = components.get('underlying_symbol')
        strike = components.get('strike_price')
        call_put = components.get('call_put')
        expiry_date = components.get('expiry_date')

        aggs = await opts.option_aggregates(ticker, timespan='second', as_dataframe=True)
        aggs = aggs.rename(columns={'v': 'volume', 'vw': 'vwap', 'o': 'open', 'h': 'high', 'l': 'low', 't': 'timestamp', 'n': 'trades'})
        aggs['timestamp'] = pd.to_datetime(aggs['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
        aggs['timestamp'] = aggs['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        aggs['option_symbol'] = ticker
        aggs['ticker'] = underlying_symbol
        aggs['strike'] = strike
        aggs['call_put'] = str(call_put).lower()
        aggs['expiry'] = expiry_date

        # Insert data into the database immediately after processing
        await opts.batch_insert_dataframe(aggs, table_name='option_aggs', unique_columns='option_symbol, timestamp')
    except Exception as e:
        print(f"Error processing ticker {ticker}: {e}")


async def process_batch(opts, tickers):
    for ticker in tickers:
        await process_ticker(opts, ticker)

async def get_all_aggregates():
    
       
    tickers = await opts.get_tickers()

    batch_size = 250  # Process 250 tickers at a time
    tasks = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        # Create an asynchronous task for each batch
        task = asyncio.create_task(process_batch(opts, batch))
        tasks.append(task)

    # Run all batch processing tasks concurrently
    await asyncio.gather(*tasks)

asyncio.run(get_all_aggregates())