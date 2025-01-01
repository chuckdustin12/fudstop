from fudstop.apis.polygonio.polygon_database import PolygonDatabase
db = PolygonDatabase()
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
poly = Polygon()
from fudstop._markets.list_sets.ticker_lists import most_active_tickers
import pandas as pd
import asyncio
from .tradier_models import Orders, Balances
from fudstop.apis.helpers import get_human_readable_string
from asyncpg import create_pool
import os
import httpx
import certifi

class TradierTrader:
    def __init__(self):
        self.key = os.environ.get('YOUR_TRADIER_KEY')
        self.access_token = 'vsOHJgJzY2CDb4Qe1U5l2gHfITGt'
        self.account_id = "6YB38617"
        self.paper_base = "https://sandbox.tradier.com"
        self.paper_account = "VA6079106"
        self.headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'  # Make sure the content type is properly set
        }
        self.pool = None
        self.host = 'localhost'
        self.password = 'fud'
        self.database = 'market_data'
        self.user = 'chuck'
        self.db=db

    async def post_data(self, url, payload):
        async with httpx.AsyncClient(headers=self.headers, verify=certifi.where()) as client:
            try:
                response = await client.post(url, data=payload)
                response.raise_for_status()  # Raise exception if the request failed
                return response.json()
            except httpx.RequestError as e:
                print(f"An error occurred while requesting: {e}")
            except httpx.HTTPStatusError as e:
                print(f"HTTP Error: {e}")


    async def get_data(self, url):
        async with httpx.AsyncClient(headers=self.headers, verify=certifi.where()) as client:
            try:
                response = await client.get(url)
                response.raise_for_status()  # Raise exception if the request failed
                return response.json()
            except httpx.RequestError as e:
                print(f"An error occurred while requesting: {e}")
            except httpx.HTTPStatusError as e:
                print(f"HTTP Error: {e}")

    async def paper_order(self, symbol, account="VA6079106", class_='equity', side='buy', quantity="1", order_type='market', duration='gtc', stop=None):
        url = self.paper_base + f"/v1/accounts/{account}/orders"
        price = await poly.get_price(symbol)
        payload = {
            'account_id': account,
            'symbol': symbol,
            'class': class_,
            'side': side,
            'quantity': quantity,
            'type': order_type,
            'duration': duration,
            'price': price,
        }
        
        if stop:
            payload['stop'] = stop

        data = await self.post_data(url, payload=payload)
        return data


    async def paper_positions(self):
        """View current Paper positions"""
        url = self.paper_base + f"/v1/accounts/{self.paper_account}/positions"

        positions = await self.get_data(url)

        positions = positions['positions']
        for i in positions:
            print(i)


    async def paper_orders(self):
        """View current paper orders"""
        url = self.paper_base + f"/v1/accounts/{self.paper_account}/orders"

        orders = await self.get_data(url)

        orders = orders['orders']

        order = orders['order']

        return Orders(order)
    

    async def balances(self):
        """View account balances"""
        url = self.paper_base + f"/v1/accounts/{self.paper_account}/balances"
        
        balances = await self.get_data(url)

        balances = balances['balances']


        return Balances(balances)


    async def lookup_options(self, ticker):

        url = self.paper_base + f"/v1/markets/options/lookup?underlying={ticker}"
        symbols = await self.get_data(url)

        if not symbols or 'symbols' not in symbols:
            raise ValueError(f"Symbols not found for ticker: {ticker}")

        symbols = symbols['symbols']
        options = [f"O:{option}" for i in symbols for option in i.get('options', [])]


        components = [get_human_readable_string(i) for i in options]

        # Extract attributes from components
        underlying = [i.get('underlying_symbol') for i in components]
        strike = [i.get('strike_price') for i in components]
        expiry = [i.get('expiry_date') for i in components]
        call_put = [i.get('call_put') for i in components]

        # Create DataFrame with the appropriate data
        df = pd.DataFrame({
            "option_symbol": options,
            "ticker": ticker,
            'strike': strike,
            'call_put': call_put,
            'expiry': expiry
        })

        # Perform batch upsert with database connection pool
        await self.db.batch_upsert_dataframe(df, table_name='symbols', unique_columns=['option_symbol'])

    async def option_store(self):
        # Establish connection pool first
        await self.db.connect()

        try:
            # Split `most_active_tickers` into batches of 25
            batch_size = 25
            batches = [most_active_tickers[i:i + batch_size] for i in range(0, len(most_active_tickers), batch_size)]

            # Process each batch of tickers
            for batch in batches:
                tasks = [self.lookup_options(ticker) for ticker in batch]
                await asyncio.gather(*tasks)
        
        finally:
            # Disconnect from the database after all batches are processed
            await self.db.disconnect()