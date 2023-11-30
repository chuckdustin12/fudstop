import sys
from pathlib import Path
from asyncpg.exceptions import UniqueViolationError
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)

from dotenv import load_dotenv
load_dotenv()
from asyncpg import create_pool
from urllib.parse import unquote
import os
from typing import List, Dict, Optional
import pandas as pd
import asyncio
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError, ClientConnectionError, ContentTypeError

from .models.aggregates import AggregatesData
from .models.ticker_news import TickerNews
from .models.company_info import CombinedCompanyResults
from .models.technicals import RSI, EMA, SMA
from .models.ticker_snapshot import StockSnapshot
from datetime import datetime, timedelta
import aiohttp

from urllib.parse import urlencode
import requests
from apis.helpers import flatten_dict

YOUR_POLYGON_KEY = os.environ.get('YOUR_POLYGON_KEY')


session = requests.session()
class Polygon:
    def __init__(self, connection_string=None):
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

    async def connect(self, connection_string=None):
            if connection_string:
                self.pool = await create_pool(
                    dsn=connection_string, min_size=1, max_size=10
                )
            else:
                self.pool = await create_pool(
                    host=os.environ.get('DB_HOST'),
                    port=os.environ.get('DB_PORT'),
                    user=os.environ.get('DB_USER'),
                    password=os.environ.get('DB_PASSWORD'),
                    database='polygon',
                    min_size=1,
                    max_size=10
                )
            return self.pool

    async def save_structured_message(self, data: dict, table_name: str):
        fields = ', '.join(data.keys())
        values = ', '.join([f"${i+1}" for i in range(len(data))])
        
        query = f'INSERT INTO {table_name} ({fields}) VALUES ({values})'
      
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(query, *data.values())
            except UniqueViolationError:
                print('Duplicate - SKipping')

    async def fetch_page(self, url):
        if 'apiKey' not in url:
            url = url + f"?apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
    async def paginate_concurrent(self, url, as_dataframe=False, concurrency=25):
        all_results = []

        
        async with aiohttp.ClientSession() as session:
            pages_to_fetch = [url]
            
            while pages_to_fetch:
                tasks = []
                
                for _ in range(min(concurrency, len(pages_to_fetch))):
                    next_url = pages_to_fetch.pop(0)
                    tasks.append(self.fetch_page(next_url))
                    
                results = await asyncio.gather(*tasks)
                if results is not None:
                    for data in results:
                        if data is not None:
                            if "results" in data:
                                all_results.extend(data["results"])
                                
                            next_url = data.get("next_url")
                            if next_url:
                                next_url += f'&{urlencode({"apiKey": f"{self.api_key}"})}'
                                pages_to_fetch.append(next_url)
                        else:
                            break
        if as_dataframe:
            import pandas as pd
            return pd.DataFrame(all_results)
        else:
            return all_results
        


    async def fetch_endpoint(self, endpoint, params=None):
        # Build the query parameters
        filtered_params = {k: v for k, v in params.items() if v is not None} if params else {}
        
        if filtered_params:
            query_string = urlencode(filtered_params)
            if query_string:
                endpoint = f"{endpoint}?{query_string}&{self.api_key}"
                
        async with aiohttp.ClientSession() as session:
            async with session.get(url=endpoint, params=filtered_params) as response:
                return await response.json()  # or response.text() based on your needs
                



    
    async def aggregates(self, ticker, multiplier:str='1', timespan:str='day', date_from=None, date_to=None, limit:str=500, sort:str='desc'):
        """
        Fetches candlestick data for a ticker, option symbol, crypto/forex pair.
        
        Parameters:
        - ticker (str): The ticker symbol for which to fetch data.

        - timespan: The timespan to survey.

        TIMESPAN OPTIONS:

        >>> second
        >>> minute
        >>> hour
        >>> day
        >>> week
        >>> month
        >>> quarter
        >>> year



        >>> Multiplier: the number of timespans to survey.

        - date_from (str, optional): The starting date for the data fetch in yyyy-mm-dd format.
                                     Defaults to 30 days ago if not provided.
        - date_to (str, optional): The ending date for the data fetch in yyyy-mm-dd format.
                                   Defaults to today's date if not provided.

        - limit: the amount of candles to return. Defaults to 500



        Returns:
        - dict: Candlestick data for the given ticker and date range.

        Example:
        >>> await aggregates('AAPL', date_from='2023-09-01', date_to='2023-10-01')
        """

        if date_from is None:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        if date_to is None:
            date_to = datetime.now().strftime('%Y-%m-%d')
        
  
        params = {
            'adjusted': 'true',
            'sort': sort,
            'limit': limit,
            'apiKey': self.api_key  # API key included here
        }

        endpoint = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}"
        if limit == 50000:
            all_data = await self.paginate_concurrent(endpoint)
            return all_data    
        data = await self.fetch_endpoint(endpoint, params=params)
        data = AggregatesData(data)

        if limit == 50000:
            all_data = await self.paginate_concurrent(endpoint)
            return all_data    
        return data


    async def market_news(self, limit: str = '100'):
        """
        Arguments:

        >>> ticker: the ticker to query (optional)
        >>> limit: the number of news items to return (optional) | Max 1000

        """
        params = {
            'apiKey': self.api_key,
            'limit': limit
        }


        endpoint = "https://api.polygon.io/v2/reference/news"

        data = await self.fetch_endpoint(endpoint, params=params)
        data = TickerNews(data)

        return data
    

    async def company_info(self, ticker) -> CombinedCompanyResults:
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"
       
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                results_data = data['results'] if 'results' in data else None
                if results_data is not None:
                    return CombinedCompanyResults(
                        ticker=results_data.get('ticker'),
                        name=results_data.get('name'),
                        market=results_data.get('market'),
                        locale=results_data.get('locale'),
                        primary_exchange=results_data.get('primary_exchange'),
                        type=results_data.get('type'),
                        active=results_data.get('active'),
                        currency_name=results_data.get('currency_name'),
                        cik=results_data.get('cik'),
                        composite_figi=results_data.get('composite_figi'),
                        share_class_figi=results_data.get('share_class_figi'),
                        market_cap=results_data.get('market_cap'),
                        phone_number=results_data.get('phone_number'),
                        description=results_data.get('description'),
                        sic_code=results_data.get('sic_code'),
                        sic_description=results_data.get('sic_description'),
                        ticker_root=results_data.get('ticker_root'),
                        homepage_url=results_data.get('homepage_url'),
                        total_employees=results_data.get('total_employees'),
                        list_date=results_data.get('list_date'),
                        share_class_shares_outstanding=results_data.get('share_class_shares_outstanding'),
                        weighted_shares_outstanding=results_data.get('weighted_shares_outstanding'),
                        round_lot=results_data.get('round_lot'),
                        address1=results_data.get('address', {}).get('address1'),
                        city=results_data.get('address', {}).get('city'),
                        state=results_data.get('address', {}).get('state'),
                        postal_code=results_data.get('address', {}).get('postal_code'),
                        logo_url=results_data.get('branding', {}).get('logo_url'),
                        icon_url=results_data.get('branding', {}).get('icon_url')
                    )
                else:
                    print(f'Couldnt get info for {ticker}')
    def company_info_sync(self, ticker) -> CombinedCompanyResults:
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"
        data = session.get(url).json()
        results_data = data['results'] if 'results' in data else None
        if results_data is not None:
            return CombinedCompanyResults(
                ticker=results_data.get('ticker'),
                name=results_data.get('name'),
                market=results_data.get('market'),
                locale=results_data.get('locale'),
                primary_exchange=results_data.get('primary_exchange'),
                type=results_data.get('type'),
                active=results_data.get('active'),
                currency_name=results_data.get('currency_name'),
                cik=results_data.get('cik'),
                composite_figi=results_data.get('composite_figi'),
                share_class_figi=results_data.get('share_class_figi'),
                market_cap=results_data.get('market_cap'),
                phone_number=results_data.get('phone_number'),
                description=results_data.get('description'),
                sic_code=results_data.get('sic_code'),
                sic_description=results_data.get('sic_description'),
                ticker_root=results_data.get('ticker_root'),
                homepage_url=results_data.get('homepage_url'),
                total_employees=results_data.get('total_employees'),
                list_date=results_data.get('list_date'),
                share_class_shares_outstanding=results_data.get('share_class_shares_outstanding'),
                weighted_shares_outstanding=results_data.get('weighted_shares_outstanding'),
                round_lot=results_data.get('round_lot'),
                address1=results_data.get('address', {}).get('address1'),
                city=results_data.get('address', {}).get('city'),
                state=results_data.get('address', {}).get('state'),
                postal_code=results_data.get('address', {}).get('postal_code'),
                logo_url=results_data.get('branding', {}).get('logo_url'),
                icon_url=results_data.get('branding', {}).get('icon_url')
            )
        else:
            print(f'Couldnt get info for {ticker}')

    async def get_all_tickers(self, include_otc=False, save_all_tickers:bool=False) -> List[StockSnapshot]:
        """
        Fetches a list of all stock tickers available on Polygon.io.

        Arguments:
            >>> include_otc: optional - whether to include OTC securities or not

            >>> save_all_tickers: optional - saves all tickers as a list for later processing

        Returns:
            A list of StockSnapshot objects, each containing data for a single stock ticker.

        Usage:
            To fetch a list of all stock tickers available on Polygon.io, you can call:
            ```
            tickers = await sdk.get_all_tickers()
            print(f"Number of tickers found: {len(tickers)}")
            ```
        """
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?include_otc={include_otc}&apiKey={self.api_key}"
        params = {
            "apiKey": self.api_key,
        }
    
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                response_data = await response.json()



                tickers = response_data['tickers']
             
                ticker_data = [StockSnapshot(ticker) for ticker in response_data['tickers'] if ticker is not None]

                if save_all_tickers:
                    # Extract tickers to a list
                    ticker_list = [ticker['ticker'] for ticker in tickers]
                    
                    # Write the tickers to a file
                    with open('list_sets/saved_tickers.py', 'w') as f:
                        f.write(str(ticker_list))
                return ticker_data


    async def rsi(self, ticker:str, timespan:str, limit:str='1000', window:int=14, date_from:str=None, date_to:str=None, session=None):
        """
        Arguments:

        >>> ticker

        >>> AVAILABLE TIMESPANS:

        minute
        hour
        day
        week
        month
        quarter
        year

        >>> date_from (optional) 
        >>> date_to (optional)
        >>> window: the RSI window (default 14)
        >>> limit: the number of N timespans to survey
        
        """

        if date_from is None:
            date_from = self.eight_days_ago

        if date_to is None:
            date_to = self.today


        endpoint = f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan={timespan}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&window={window}&apiKey={self.api_key}"
 
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(endpoint) as resp:
                    datas = await resp.json()
                    if datas is not None:

                        
  
                
                        return RSI(datas, ticker)
            except (ClientConnectorError, ClientOSError, ContentTypeError):
                print(f"ERROR - {ticker}")


    async def rsi_snapshot(self, ticker:str):
        timespans = ['minute', 'hour', 'day', 'week', 'month']
        all_data_dicts=[]
        for timespan in timespans:
            rsi =await self.rsi(ticker, timespan, limit='1')

            data_dict = { 
                'ticker': ticker,
                'rsi': rsi.rsi_value[0]
            }
            all_data_dicts.append(data_dict)
        df = pd.DataFrame(all_data_dicts)
        
        return df


    async def sma(self, ticker:str, timespan:str, limit:str='1000', window:str='9', date_from:str=None, date_to:str=None):
        """
        Arguments:

        >>> ticker

        >>> AVAILABLE TIMESPANS:

        minute
        hour
        day
        week
        month
        quarter
        year

        >>> date_from (optional) 
        >>> date_to (optional)
        >>> window: the SMA window (default 9)
        >>> limit: the number of N timespans to survey
        
        """

        if date_from is None:
            date_from = self.eight_days_ago

        if date_to is None:
            date_to = self.today


        endpoint = f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan={timespan}&window={window}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&apiKey={self.api_key}"

        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as resp:
                datas = await resp.json()


                return SMA(datas, ticker)
            


    async def ema(self, ticker:str, timespan:str, limit:str='1', window:str='21', date_from:str=None, date_to:str=None):
        """
        Arguments:

        >>> ticker

        >>> AVAILABLE TIMESPANS:

        minute
        hour
        day
        week
        month
        quarter
        year

        >>> date_from (optional) 
        >>> date_to (optional)
        >>> window: the EMA window (default 21)
        >>> limit: the number of N timespans to survey
        
        """

        if date_from is None:
            date_from = self.eight_days_ago

        if date_to is None:
            date_to = self.today


        endpoint = f"https://api.polygon.io/v1/indicators/ema/{ticker}?timespan={timespan}&window={window}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&apiKey={self.api_key}"

        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as resp:
                datas = await resp.json()


                return EMA(datas, ticker)
            




    async def get_universal_snapshot(self, ticker, retries=3): #✅
        """Fetches the Polygon.io universal snapshot API endpoint"""
        timeout = aiohttp.ClientTimeout(total=10)  # 10 seconds timeout for the request
        
        for retry in range(retries):
        # async with sema:
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&apiKey={self.api_key}&limit=250"

            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        results = data.get('results', None)
        
                        if results is not None:
                            flattened_results = [flatten_dict(result) for result in results]
                            return flattened_results
                            
                except aiohttp.ClientConnectorError:
                    print("ClientConnectorError occurred. Retrying...")
                    continue
                
                except aiohttp.ContentTypeError as e:
                    print(f"ContentTypeError occurred: {e}")  # Consider logging this
                    continue
                
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")  # Consider logging this
                    continue





    async def gather_rsi_for_all_tickers(self, tickers) -> List[RSI]:

        """Get RSI for all tickers
        
        Arguments:

        >>> tickers: A list of tickers


        >>> timespan: 

           minute
           hour
           day
           week
           month
           year
           quaeter
        
        """
        async with aiohttp.ClientSession() as session:
            timespans = ['minute', 'hour', 'day', 'week']
            timespan = 'hour'
            tasks = [self.rsi(ticker, timespan) for ticker in tickers for timespan in timespans]
            await asyncio.gather(*tasks)
                
                
    async def get_polygon_logo(self, symbol: str) -> Optional[str]:
            """
            Fetches the URL of the logo for the given stock symbol from Polygon.io.

            Args:
                symbol: A string representing the stock symbol to fetch the logo for.

            Returns:
                A string representing the URL of the logo for the given stock symbol, or None if no logo is found.

            Usage:
                To fetch the URL of the logo for a given stock symbol, you can call:
                ```
                symbol = "AAPL"
                logo_url = await sdk.get_polygon_logo(symbol)
                if logo_url is not None:
                    print(f"Logo URL: {logo_url}")
                else:
                    print(f"No logo found for symbol {symbol}")
                ```
            """
            url = f'https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={self.api_key}'
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    data = await response.json()
                    
                    if 'results' not in data:
                        # No results found
                        return None
                    
                    results = data['results']
                    branding = results.get('branding')

                    if branding and 'icon_url' in branding:
                        encoded_url = branding['icon_url']
                        decoded_url = unquote(encoded_url)
                        url_with_api_key = f"{decoded_url}?apiKey={self.api_key}"
                        return url_with_api_key