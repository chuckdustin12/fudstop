import os
from dotenv import load_dotenv
load_dotenv()
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
import asyncpg


###DATABASE INSERTION FOR POLYGON###

class GroupedAggsModel:
    def __init__(self):
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today = datetime.now().strftime('%Y-%m-%d')
    async def fetch_stock_data(self, date=None, adjusted=True, include_otc=False):
        if date is None:
            date = self.today
        url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
        params = {
            "adjusted": str(adjusted).lower(),
            "include_otc": str(include_otc).lower(),
            "apiKey": self.api_key
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
                df = pd.DataFrame(data['results'])
                df['timestamp'] = pd.to_datetime(df['t'], unit='ms')  # Adjust the unit if necessary
                return df.drop(columns=['t'])

    async def create_table(self, conn):
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_aggregates (
                T TEXT,
                V BIGINT,
                VW REAL,
                O REAL,
                C REAL,
                H REAL,
                L REAL,
                N BIGINT,
                Timestamp TIMESTAMP WITHOUT TIME ZONE
            )
        """)

    async def insert_data(self, conn, data_frame):
        for index, row in data_frame.iterrows():
            await conn.execute("""
                INSERT INTO daily_aggregates (T, V, VW, O, C, H, L, N, Timestamp) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, row['T'], row['v'], row['vw'], row['o'], row['c'], row['h'], row['l'], row['n'], row['timestamp'])

# aggs_model = GroupedAggsModel()
# loop = asyncio.get_event_loop()
# connection = loop.run_until_complete(asyncpg.connect(user=os.environ.get('DB_USER'), password=os.environ.get('DB_PASSWORD'), database='polygon', host=os.environ.get('DB_HOST')))
# loop.run_until_complete(aggs_model.create_table(connection))
# df = loop.run_until_complete(aggs_model.fetch_stock_data())
# loop.run_until_complete(aggs_model.insert_data(connection, df))

# ##############################

