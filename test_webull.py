import os
from dotenv import load_dotenv
load_dotenv()



import asyncio

from fudstop.list_sets.ticker_lists import most_active_tickers
from fudstop.apis.webull.webull_options import WebullOptions

opts = WebullOptions(connection_string=os.environ.get('WEBULL_OPTIONS'))

import aiohttp
import requests

async def main():
    await opts.connect()
    async for id in opts.yield_batch_ids('SPY'):
        ids = id.split(',')
        for id in ids:
            url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId=1038922093"
            r = requests.get(url, headers=opts.headers).json()
            print(r) 


asyncio.run(main())