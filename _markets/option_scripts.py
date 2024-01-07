import os

from dotenv import load_dotenv
load_dotenv()
import sys
from pathlib import Path
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from database_ import DatabaseManager
import asyncio


class OptionScripts():
    def __init__(self):
        self.db = DatabaseManager(host='localhost', user='chuck', port=5432, password='fud', database='markets')



        
    async def yield_ticker(self):
        await self.db.connect()
        buffer = []
        while True:
            query = """SELECT DISTINCT option_symbol FROM options_data WHERE expiry > '2024-01-06';"""
            async for record in self.db.fetch_iter(query):
                ticker = record['option_symbol']


                buffer.append(ticker)
                if len(buffer) >= 250:
                    yield ','.join(buffer)
                    buffer = []


scripts = OptionScripts()
async def main():
    async for ticker in scripts.yield_ticker():
        print(ticker)

asyncio.run(main())