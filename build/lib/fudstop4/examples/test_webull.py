import os
from dotenv import load_dotenv
load_dotenv()
import math
from scipy.stats import norm
from datetime import datetime

import numpy as np
import asyncio
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
from fudstop4.apis.webull.webull_options import WebullOptions
from fudstop4.apis.webull.webull_trading import WebullTrading
db_config = {
    "host": os.environ.get('DB_HOST', 'localhost'), # Default to this IP if 'DB_HOST' not found in environment variables
    "port": int(os.environ.get('DB_PORT')), # Default to 5432 if 'DB_PORT' not found
    "user": os.environ.get('DB_USER', 'postgres'), # Default to 'postgres' if 'DB_USER' not found
    "password": os.environ.get('DB_PASSWORD', 'fud'), # Use the password from environment variable or default
    "database": os.environ.get('DB_NAME', 'polygon') # Database name for the new jawless database
}
opts = WebullOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')
poly = PolygonOptions(database='fudstop3')
wt = WebullTrading()
import pandas as pd
import aiohttp
import requests

######### YOU MUST EXPAND HERE

async def main():
    counter = 0
    async for quote in wt.multi_quote(['CHWY', 'M', 'ENPH', 'MRNA', 'JD']):
        counter += 1
        quote = quote.split('|')
        sym = quote[1]
        price = quote[3]
        vol = quote[5]
        vibration = quote[7]

        print(f"{sym} | ${price} | {vol} | {vibration}")
        # Perform a specific action every 300 iterations
        if counter % 300 == 0:
            # Perform some action here
            print("Action performed after 300 iterations.")

        # The loop will continue after the action

asyncio.run(main())