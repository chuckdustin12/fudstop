from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.webull.webull_markets import WebullMarkets
wbm = WebullMarkets()
import pandas as pd
from fudstop4.apis.helpers import generate_webull_headers
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
opts = WebullOptions()


import asyncio



async def main():

    x = await wbm.tc_summaries()


    print(x)

asyncio.run(main())