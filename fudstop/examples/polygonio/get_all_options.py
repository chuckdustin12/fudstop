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
from fudstop._markets.list_sets.ticker_lists import all_tickers, most_active_tickers
import pandas as pd

# Setting environment variables
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PASSWORD'] = 'fud'
os.environ['DB_NAME'] = 'polygon'
os.environ['DB_PORT'] = '5432'  # Environment variables are stored as strings
os.environ['DB_USER'] = 'chuck'

# Database configuration using environment variables
db_config = {
    "host": os.environ.get('DB_HOST', 'localhost'),
    "port": int(os.environ.get('DB_PORT', 5432)),
    "user": os.environ.get('DB_USER', 'postgres'),
    "password": os.environ.get('DB_PASSWORD', 'fud'),
    "database": os.environ.get('DB_NAME', 'polygon')
}


opts = PolygonOptions()
from os.path import exists
from asyncio import Semaphore, Lock
from asyncpg.exceptions import UniqueViolationError
sema = Semaphore(15)
file_lock = Lock()
async def get_all_options(ticker, ticker_list):
    async with sema:
        options = await opts.get_option_chain_all(ticker)
        df = pd.DataFrame(options.data_dict)
        try:
            await opts.batch_insert_dataframe(df, 'poly_opts', unique_columns='option_symbol', batch_size=1000)
            ticker_list.append(ticker)

            # Handle CSV writing with a lock
            async with file_lock:
                if not exists('files/all_option_data.csv'):
                    df.to_csv('files/all_option_data.csv', index=False)
                else:
                    df.to_csv('files/all_option_data.csv', mode='a', header=False, index=False)

        except UniqueViolationError:
            print(f'Skipping {ticker} - Already exists.')
        except Exception as e:
            print(f'Error with {ticker}: {e}')

async def run_all_options():
    await opts.connect()

    
    try:
        ticker_list = []
        tasks = [get_all_options(ticker, ticker_list) for ticker in most_active_tickers]
        await asyncio.gather(*tasks)
    except Exception as e:
        print(f'Error - {e}')

asyncio.run(run_all_options())