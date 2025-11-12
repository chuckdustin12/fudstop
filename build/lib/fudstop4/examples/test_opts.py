import asyncio
from fudstop4.apis.polygonio.polygon_options import PolygonOptions

from fudstop4._markets.list_sets.ticker_lists import most_active_tickers
opts = PolygonOptions(database='fudstop3')

async def main(ticker, semaphore):
    async with semaphore:
        all_options = await opts.find_symbols(ticker)
        df = all_options.df
        print(df)
        await opts.batch_insert_dataframe(df, table_name='poly_options', unique_columns='option_symbol')

async def update_all_options():
    pool = await opts.connect()
    semaphore = asyncio.Semaphore(3)  # Adjust the number as needed
    tasks = [main(i, semaphore) for i in most_active_tickers]
    await asyncio.gather(*tasks)

asyncio.run(update_all_options())