import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import asyncio
import pandas as pd
from dotenv import load_dotenv
from apis.webull.webull_options import WebullOptions
from apis.helpers import get_human_readable_string

load_dotenv()
import os
options = WebullOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')

import asyncio
import pandas as pd
from dotenv import load_dotenv
from apis.webull.webull_options import WebullOptions
from apis.helpers import get_human_readable_string

load_dotenv()



async def fetch_volume_analysis(option_id, ticker):
    try:
        # Asynchronous database query
        symbol_query = f"SELECT option_symbol FROM wb_opts WHERE ticker_id = {option_id}"
        symbol_result = await options.fetch(symbol_query)

        if symbol_result and 'option_symbol' in symbol_result[0]:
            option_symbol = symbol_result[0]['option_symbol']
            components = get_human_readable_string(option_symbol)
            print(components)
            # Asynchronous fetch for volume analysis
            data_frame = await options.fetch_volume_analysis(option_symbol=option_symbol, id=option_id, underlying_ticker=ticker)
            if data_frame is not None:
                data_frame['option_symbol'] = option_symbol
                data_frame['underlying_symbol'] = components.get('underlying_symbol')
                data_frame['strike_price'] = components.get('strike_price')
                data_frame['call_put'] = components.get('call_put')
                data_frame['expire_date'] = components.get('expiry_date')
                await options.batch_insert_dataframe(data_frame,table_name='vol_anal', unique_columns='option_symbol')
                return data_frame
    except Exception as e:
        print(f"Error processing option ID {option_id}: {e}")

async def process_batch(batch, ticker):
    return await asyncio.gather(*[fetch_volume_analysis(option_id, ticker) for option_id in batch])

async def main(ticker):
    

    option_ids = await options.get_option_id_for_symbol(ticker)

    batch_size = 100  # Adjust based on performance
    batches = [option_ids[i:i + batch_size] for i in range(0, len(option_ids), batch_size)]

    all_dataframes = []
    for batch in batches:
        dataframes = await process_batch(batch, ticker)
        all_dataframes.extend(df for df in dataframes if df is not None)

    await options.batch_insert_dataframe(all_dataframes,table_name='vol_anal', unique_columns='option_symbol')
    # final_dataframe = pd.concat(all_dataframes, ignore_index=True)
    # final_dataframe.to_csv('SPY_OPTIONS.csv', index=False)


async def run_main():
    await options.connect()
    tasks = [main(i) for i in options.most_active_tickers]

    await asyncio.gather(*tasks)

asyncio.run(run_main())
