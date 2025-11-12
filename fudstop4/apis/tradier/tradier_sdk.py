from .tradier_models import Orders, Balances
import os
from dotenv import load_dotenv
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(database='fudstop3')
load_dotenv()
import pandas as pd
import httpx
import certifi


class TradierSDK:
    def __init__(self):
        self.key = os.environ.get('YOUR_TRADIER_KEY')
        self.access_token = 'Hz3fy7rwSiRiNuHrLsdQOEQMYx3V'
        self.account_id = "6YB38617"
        self.base = "https://api.tradier.com"
        self.headers = {
            'Authorization': f"Bearer {self.access_token}",
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'  # Make sure the content type is properly set
        }
        self.pool = None
        self.host = 'localhost'
        self.password = 'fud'
        self.database = 'fudstop3'
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

    async def get_expirations(self, ticker):
        url=f"https://api.tradier.com/v1/markets/options/expirations?symbol={ticker}&includeAllRoots=true"

        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=self.headers)
            print(data)
            if data.status_code == 200:
                data = data.json()
                expirations = data['expirations']
                data = expirations['date']
                return data
            


    async def quotes(self, greeks: bool = True, symbols_string='SPX'):
        url = "https://api.tradier.com/v1/markets/quotes"
        payload = {
            "greeks": str(greeks).lower(),
            "symbols": f"{symbols_string}"

        }

        async with httpx.AsyncClient() as client:
            data = await client.post(url, headers=self.headers, data=payload)  # use 'data' for form-encoded payload
            if data.status_code == 200:
                json_data = data.json()
                print(json_data)
            else:
                print(f"Request failed with status code {data.status_code}")

    async def options_lookup(self, ticker:str):
        ticker= ticker.upper()
        url = f"https://api.tradier.com/v1/markets/options/lookup?underlying={ticker}"

        data = await self.get_data(url)

        symbols = data['symbols']
        options = [i.get('options') for i in symbols]

        return options
    

    async def get_quote(self, symbols_string):
        url = f"https://api.tradier.com/v1/markets/quotes?symbols={symbols_string}&greeks=true"

        data = await self.get_data(url)
        quotes = data['quotes']
        quote = quotes['quote']
        return quote


    async def get_chain(self, symbol, expiration):
        url=f"https://api.tradier.com/v1/markets/options/chains?symbol={symbol}&greeks=true&expiration={expiration}"


        data = await self.get_data(url)
        options = data['options']
        option = options['option']

        df = pd.DataFrame(option)

        return df
    

    async def get_stats(self, symbols):
        url = f"https://api.tradier.com/beta/markets/fundamentals/statistics?symbols={symbols}"

        data = await self.get_data(url)
        results = [i.get('results') for i in data]
        flat_results = [item for sublist in results for item in sublist]
        tables = [i.get('tables') for i in flat_results]
        for i in tables[0]:
            print(i)

    async def get_balance(self):
        url = self.base + f"/v1/accounts/{self.account_id}/balances"

        data = await self.get_data(url)
        balances = data['balances']
        return Balances(balances)
    

    async def get_orders(self):
        """View current paper orders"""
        url = self.base + f"/v1/accounts/{self.account_id}/orders"

        orders = await self.get_data(url)

        orders = orders['orders']

        order = orders['order']

        return Orders(order)
    


    async def gain_loss(self):
        """View gains and losses"""
        try:
            url = self.base + f"/v1/accounts/{self.account_id}/gainloss"


            data = await self.get_data(url)
            gainloss = data['gainloss']
            closed_position = gainloss['closed_position'] if gainloss != 'null' else 'No history'

            for i in closed_position[0]:
                print(f"self.{i} = [i.get('{i}') for i in closed_position]")
        except Exception as e:
            print(f"No history.")