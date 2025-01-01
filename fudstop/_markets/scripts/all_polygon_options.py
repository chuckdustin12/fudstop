
from dotenv import load_dotenv
load_dotenv()
import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

from fudstop.apis.polygonio.polygon_options import PolygonOptions
from list_sets.ticker_lists import most_active_tickers
import asyncio


db = PolygonOptions(user='chuck', database='market_data')
async def main(ticker):
    
    all_options = await db.get_option_chain_all(underlying_asset=ticker)

    asyncio.create_task(db.batch_insert_dataframe(all_options.df,table_name='opts', unique_columns='option_symbol'))
async def run_main():
    await db.connect()
    tasks = [main(i) for i in most_active_tickers]


    await asyncio.gather(*tasks)



asyncio.run(run_main())