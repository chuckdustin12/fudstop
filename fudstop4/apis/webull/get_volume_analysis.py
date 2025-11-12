import os
from dotenv import load_dotenv
load_dotenv()

from apis.webull.webull_options import WebullOptions
from apis.helpers import get_human_readable_string, rename_keys
import asyncpg


wb = WebullOptions(user='chuck', database='charlie', host='localhost', port=5432, password='fud')



import asyncio
async def collect_ticker_symbol_pairs(generator):
    ticker_symbol_dict = {}
    async for ticker_id, symbol in generator:
        ticker_symbol_dict[ticker_id] = symbol
    return ticker_symbol_dict

# Assume 'self' has the 'get_ticker_symbol_pairs' method
async def main():
    await wb.connect()
    ticker_symbol_pairs_generator = wb.get_ticker_symbol_pairs()
    ticker_symbol_dict = await collect_ticker_symbol_pairs(ticker_symbol_pairs_generator)
    # Now ticker_symbol_dict contains all the ticker_id and symbol pairs

    for k,v in ticker_symbol_dict.items():
        vol_anal = await wb.fetch_volume_analysis(k)
        
        data_dict = vol_anal.data_dict
        try:
            trades_and_dates = vol_anal.flattened_trade

            print(trades_and_dates)
        except AttributeError:
            continue


        components = get_human_readable_string(f"O:{v}")
        print(components)

        data_dict.update({"option_symbol": f"{v}", "symbol": f"{components.get('underlying_symbol')}", "strike_price": f"{float(components.get('strike_price'))}", "call_put": f"{components.get('call_put')}", "expiry_date": f"{components.get('expiry_date')}"})
        trades_and_dates.update({"option_symbol": f"{v}", "symbol": f"{components.get('underlying_symbol')}", "strike_price": f"{float(components.get('strike_price'))}", "call_put": f"{components.get('call_put')}", "expiry_date": f"{components.get('expiry_date')}"})

        print(trades_and_dates)
        # Define your key mapping 
        key_mapping = {
            'tickerId': 'option_id',
            'belongTickerId': 'ticker_id',
            'totalNum': 'total_trades',
            'totalVolume': 'total_volume',
            'avgPrice': 'average_price',
            'buyVolume': 'buy_volume',
            'sellVolume': 'sell_volume',
            'neutralVolume': 'neutral_volume',
           
        }

        # Rename the keys
        renamed_data = rename_keys(data_dict, key_mapping)
        try:
            await wb.insert_trades_and_dates(trades_and_dates)
            await wb.insert_volume_analysis_data(renamed_data)
        except asyncpg.exceptions.NotNullViolationError:
            print(f'Null detected. Skipping insert.')
 
        


        


# Run the main function in the asyncio event loop
import asyncio
# assuming 'self' is an instance of the class containing the get_ticker_symbol_pairs method
asyncio.run(main())
