import os
from dotenv import load_dotenv
from apis.helpers import get_human_readable_string
load_dotenv()

from apis.webull.webull_options import WebullOptions
import pandas as pd
options = WebullOptions(connection_string=os.environ.get('WEBULL_OPTIONS'))
import asyncio

async def fetch_and_print_volume_analysis(semaphore, option_id):
    async with semaphore:
        data = await options.fetch_volume_analysis(option_id)
        symbol_query = f"SELECT symbol FROM webull_options WHERE ticker_id = '{option_id}'"
        symbol_result = await options.fetch(symbol_query)
        option_symbol = symbol_result[0]['symbol'] if symbol_result and symbol_result[0].get('symbol') is not None else 'Unknown'
        components = get_human_readable_string(option_symbol)

        strike_price = components.get('strike_price')
        expiry_date = components.get('expiry_date')
        call_put = components.get('call_put')
        symbol = components.get('underlying_symbol')
        data_frame = data.as_dataframe
        data_frame['option_symbol'] = option_symbol
        data_frame['symbol'] = symbol
        data_frame['strike_price'] = strike_price
        data_frame['call_put'] = call_put,
        data_frame['expiry_date'] = expiry_date
        return data_frame

async def main():
    await options.connect()
    option_ids = await options.get_option_id_for_symbol('SPY')
    semaphore = asyncio.Semaphore(35)
    tasks = [fetch_and_print_volume_analysis(semaphore, option_id) for option_id in option_ids]
    dataframes = await asyncio.gather(*tasks)
    final_dataframe = pd.concat(dataframes, ignore_index=True)
    print(final_dataframe)

    final_dataframe.to_csv('SPY_OPTIONS.csv', index=False)





asyncio.run(main())