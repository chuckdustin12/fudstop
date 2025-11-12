from fudstop4.apis.ultimate.ultimate_sdk import UltimateSDK
from fudstop4.apis.webull.webull_options.webull_options import WebullOptions
from fudstop4.apis.webull.webull_trading import WebullTrading
trading = WebullTrading()
opts = WebullOptions()

ultim = UltimateSDK()
import requests
import asyncio
from fudstop4.apis.helpers import generate_webull_headers
from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
import aiohttp
async def main():

    news = await trading.ai_news('AAPL', page_size='1', headers=generate_webull_headers())

    print(news.as_dataframe)
asyncio.run(main())