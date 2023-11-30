import os

from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from fudstop.apis.polygonio.polygon_options import PolygonOptions
from tabulate import tabulate
from fudstop.apis.helpers import format_large_numbers_in_dataframe
from fudstop.apis.polygonio.polygon_helpers import get_human_readable_string
from fudstop.list_sets.ticker_lists import most_active_tickers
from fudstop.apis.gexbot.gexbot import GEXBot
opts = PolygonOptions()
gex = GEXBot()
import asyncio




async def all_options(ticker):
    try:
        data = await opts.get_option_chain_all(ticker)
        df = data.df
        
        await opts.batch_insert_dataframe(df, table_name='all_options', unique_columns='option_symbol')
    except Exception as e:
        print(e)


async def all_aggs():
    async for ticker in opts.yield_tickers():

        aggs = await opts.option_aggregates(ticker, timespan='minute')
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
        aggs['call_put'] = str(call_put).lower
        aggs['expiry'] = expiry_date

        print(aggs)
        df = pd.DataFrame(aggs)
        await opts.batch_insert_dataframe(df, table_name='optionaggs', unique_columns='option_symbol, timestamp')
async def run_main():
    await opts.connect()

    tasks = [all_options(i) for i in most_active_tickers]
    completed_tasks = []

    for completed in asyncio.as_completed(tasks):
        result = await completed
        completed_tasks.append(result)

    # Do something with completed_tasks if needed

asyncio.run(all_aggs())