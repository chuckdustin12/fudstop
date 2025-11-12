import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)

import os

from dotenv import load_dotenv
load_dotenv()


import asyncio



from apis.polygonio.polygon_options import PolygonOptions


poly = PolygonOptions(database='fudstop3')

async def process_rsi(opts, ticker):
    try:
        rsi_snapshot= await opts.rsi_snapshot(ticker)
        print(rsi_snapshot)
        await opts.batch_insert_dataframe(rsi_snapshot, 'rsi', unique_columns='option_symbol, timespan')
    except Exception as e:
        print(e)

async def process_batch(opts, tickers):
    for ticker in tickers:
        await process_rsi(opts, ticker)
async def main():
    await poly.connect()
    tickers = await poly.get_tickers()

    batch_size = 250  # Process 250 tickers at a time
    tasks = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        # Create an asynchronous task for each batch
        task = asyncio.create_task(process_batch(poly, batch))
        tasks.append(task)

    await asyncio.gather(*tasks)



asyncio.run(main())