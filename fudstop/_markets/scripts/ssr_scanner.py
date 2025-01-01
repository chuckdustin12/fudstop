from fudstop.apis.webull.webull_trading import WebullTrading
from list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)

trading = WebullTrading()

from discord_webhook import AsyncDiscordWebhook, DiscordEmbed

import asyncio
import os
from dotenv import load_dotenv
load_dotenv()

async def ssr_feed(ticker):

    """Scans a ticker for a specific condition"""


    stock_quote = await trading.stock_quote(ticker)
    change_ratio = stock_quote.web_change_ratio

    #conver to percentage from decimal
    if change_ratio is not None:
        change_ratio = round(float(change_ratio)*100,2)
        

        if change_ratio <= -10:
            print(f"{ticker} is currently on SSR. (declined by -10% or more) with a change ratio of {change_ratio}%")


            hook = AsyncDiscordWebhook(os.environ.get('ssr_scanner'),
                                        content=f"{ticker} is currently on SSR. (declined by -10% or more) with a change ratio of {change_ratio}%")

            await hook.execute()



async def run_ssr_feed():

    tasks = [ssr_feed(i) for i in most_active_tickers]

    await asyncio.gather(*tasks)


asyncio.run(run_ssr_feed())