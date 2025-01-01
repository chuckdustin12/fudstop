import os

from dotenv import load_dotenv
load_dotenv()
import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from market_handlers.database_ import MarketDBManager
import asyncio


class OptionScripts():
    def __init__(self):
        self.db = MarketDBManager(host='localhost', user='chuck', port=5432, password='fud', database='market_data')



        
    async def yield_multiple_symbols(self):
        await self.db.connect()
        buffer = []
        query = """SELECT DISTINCT option_symbol FROM wb_opts WHERE expire_date > '2024-01-12';"""
        while True:
            
            async for record in self.db.fetch_iter(query):
                ticker = record['option_symbol']


                buffer.append(ticker)
                if len(buffer) >= 250:
                    yield ','.join(buffer)
                    buffer = []


    async def yield_single_symbol(self):
        await self.db.connect()
        query = """SELECT DISTINCT option_symbol FROM wb_opts WHERE expire_date > '2024-01-12';"""
        while True:
            
            async for record in self.db.fetch_iter(query):
                ticker = record['option_symbol']
                yield f"O:{ticker}"

scripts = OptionScripts()
async def main():
    async for ticker in scripts.yield_single_symbol():
        print(ticker)

asyncio.run(main())