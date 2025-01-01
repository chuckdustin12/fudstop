import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import os
from dotenv import load_dotenv
load_dotenv()
from async_polygon_sdk import Polygon



import asyncio
from datetime import datetime, timedelta
class RSIScanner(Polygon):
    def __init__(self, connection_string):
        self.polygon = Polygon(connection_string)
        self.connection_string = connection_string
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')




    async def rsi_scanner(self, ticker):
        rsi = await self.rsi(ticker=ticker, timespan='hour')

        if rsi is not None:
            rsi = rsi.rsi_value[0]
            print(rsi)


scanner = RSIScanner(os.environ.get('POLYGON_STRING'))
async def main():
    results = await scanner.rsi_scanner('AAPL')
    print(results)

asyncio.run(main())



