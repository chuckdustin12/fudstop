from fudstop.apis.polygonio.polygon_options import PolygonOptions
from fudstop.apis.webull.webull_trading import WebullTrading
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
trading = WebullTrading()
import asyncio
import asyncpg
from fudstop._markets.list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)
opts = PolygonOptions(host='localhost', user='chuck', database='markets', port=5432, password='fud')
import pandas as pd
import requests



async def database_getter():
    queries = [ 
        f"""SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM fire_sale order by insertion_timestamp DESC limit 5;""",
        f"""SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM neutral_zone order by insertion_timestamp DESC limit 5;""",
        f"""SELECT ticker, avg_price, buy_pct, sell_pct, neut_pct FROM accumulation order by insertion_timestamp DESC limit 5;""",
        f"""SELECT ticker, fifty_high, close, fifty_low, change_percent FROM above_avg_vol order by insertion_timestamp DESC limit 5;""",
        f"""SELECT ticker, fifty_high, close, fifty_low, change_percent FROM below_avg_vol order by insertion_timestamp DESC limit 5;""",
        f"""SELECT ticker, fifty_high, close, fifty_low, change_percent FROM near_high order by insertion_timestamp DESC limit 5;""",
        f"""SELECT ticker, fifty_high, close, fifty_low, change_percent FROM near_low order by insertion_timestamp DESC limit 5;""",

    ]
    while True:
        async for record in opts.fetch_iter():


            return record


        async def run_database_getter():
            await opts.connect()




            tasks = [database_getter(i) for i in queries]




            await asyncio.gather(*tasks)

asyncio.run(database_getter())

