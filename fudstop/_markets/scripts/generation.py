"""USE THIS FILE TO GENERATE LISTS OF TICKERS AS NEEDED FOR BATCH CALLS TO FUNCTIONS"""


from apis.webull.webull_markets import WebullMarkets


markets = WebullMarkets()


import asyncio
import pandas as pd



async def generate_tickers_list():
    types = ['totalVolume', 'totalPosition']
    tasks = [markets.get_top_options(type) for type in types]
    results = await asyncio.gather(*tasks)
    all_symbols = []
    
    # Assuming ticker_list is a list of dictionaries
    for entry in results:
        symbols_series = pd.Series(entry.get('symbol'))
        all_symbols.append(symbols_series)
    
    # Concatenate all the Series into one and convert to a list
    all_symbols_concatenated = pd.concat(all_symbols, ignore_index=True)
    symbols_list = all_symbols_concatenated.tolist()
    
    print(symbols_list)
    # Write to a Python file
    with open('list_sets/ticker_lists.py', 'w') as f:
        f.write("most_active_tickers = " + str(list(symbols_list)))

    print(f"Written {len(set(symbols_list))} unique tickers to ticker_lists.py")

    return set(symbols_list)


asyncio.run(generate_tickers_list()) #now you can import this ticker list to run analysis on multiple tickers if needed