import sys
from pathlib import Path
from asyncpg.exceptions import UniqueViolationError
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
import httpx
from dotenv import load_dotenv
load_dotenv()
from .models.company_info import CompanyResults
from asyncpg import create_pool
from urllib.parse import unquote
import os
from fudstop.apis.helpers import format_large_numbers_in_dataframe
from typing import List, Dict, Optional
import pandas as pd
import asyncio
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError, ClientConnectionError, ContentTypeError
from .models.quotes import StockQuotes,LastStockQuote
from .models.aggregates import Aggregates
from .models.ticker_news import TickerNews

from .models.technicals import RSI, EMA, SMA, MACD
from .models.gainers_losers import GainersLosers
from .models.ticker_snapshot import StockSnapshot, SingleStockSnapshot
from .models.trades import TradeData, LastTradeData
from .models.daily_open_close import DailyOpenClose


from datetime import datetime, timedelta
import aiohttp

from urllib.parse import urlencode
import requests
from fudstop.apis.helpers import flatten_dict

YOUR_POLYGON_KEY = os.environ.get('YOUR_POLYGON_KEY')
todays_date = datetime.now().strftime('%Y-%m-%d')
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d') 
ten_days_ago = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d') 

session = requests.session()
class Polygon:
    def __init__(self, host='localhost', user='chuck', database='market_data', password='fud', port=5432):
        self.host=host
        self.indices_list = ['NDX', 'RUT', 'SPX', 'VIX', 'XSP']
        self.port=port
        self.user=user
        self.password=password
        self.database=database
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

        self.timeframes = ['minute', 'hour','day', 'week', 'month']
        self.session = None


    async def create_session(self):
        # Reuse the same session for all requests to benefit from connection pooling
        if self.session is None or self.session.is_closed:
            self.session = httpx.AsyncClient(http2=True, timeout=30.0)

    async def close_session(self):
        if self.session is not None and not self.session.is_closed:
            await self.session.aclose()
            self.session = None

    async def get_prices(self, tickers):
        """Get the prices of multiple tickers in a single API call."""
        try:
            # Prepare the tickers string for the API call
            tickers = [f"I:{ticker}" if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX'] else ticker for ticker in tickers]
            tickers_str = ",".join(tickers)
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={tickers_str}&apiKey={self.api_key}"
            await self.create_session()
            response = await self.session.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            prices = {}
            for item in results:
                ticker = item.get('ticker')
                session_data = item.get('session', {})
                close_price = session_data.get('close')
                prices[ticker] = close_price
            return prices
        except Exception as e:
            print(f"Error fetching prices: {e}")
            return {}


    async def fetch_endpoint(self, url):
        await self.create_session()  # Ensure session is created
        async with self.session.get(url) as response:
            response.raise_for_status()  # Raises exception for HTTP errors
            return await response.json()
    async def connect(self, connection_string=None):
        if connection_string:
            self.pool = await create_pool(
                host=self.host,database=self.database,password=self.password,user=self.user,port=self.port, min_size=1, max_size=30
            )
        else:
            self.pool = await create_pool(
                host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                database='market_data',
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




    async def fetch_page(self, url: str):
        """Fetch a single page of data from the provided URL."""
        # Ensure session is initialized
        if self.session is None:
            await self.create_session()

        try:
            response = await self.session.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            # Log error, handle retries if needed
            print(f"Error fetching {url}: {e}")
            return None
            
    async def stock_quotes(self, ticker:str, limit:str='50000'):
        """Gets stock quotes. Limit default is 50000"""
 
        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://api.polygon.io/v3/quotes/{ticker}?limit={limit}&apiKey={self.api_key}")

            data = data.json()
            results = data['results']

            return StockQuotes(results)


    async def last_stock_quote(self, ticker:str):
        """Get the latest quote for a ticker."""
        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://api.polygon.io/v2/last/nbbo/{ticker}?apiKey={self.api_key}")

            data = data.json()
            results = data['results']
            return LastStockQuote(results)
    async def paginate_concurrent(self, url: str, as_dataframe: bool = False, concurrency: int = 250, filter=None):
        """
        Paginate through multiple pages concurrently from a Polygon.io endpoint.
        
        :param url: Starting URL with an apiKey included.
        :param as_dataframe: If True, return results as a Pandas DataFrame.
        :param concurrency: Maximum number of concurrent requests.
        :param filter: Optional function to filter results. It should accept a list of results and return a filtered list.
        :return: List of combined results or a DataFrame if as_dataframe=True.
        """
        # Ensure we have a session
        await self.create_session()

        all_results = []
        pages_to_fetch = [url]
        sem = asyncio.Semaphore(concurrency)

        async def fetch_with_sem(page_url):
            async with sem:
                return await self.fetch_page(page_url)

        while pages_to_fetch:
            # Fetch all current pages concurrently
            tasks = [fetch_with_sem(page_url) for page_url in pages_to_fetch]
            pages_to_fetch = []  # Reset for the next loop

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for data in results:
                if isinstance(data, Exception):
                    # Log the exception if desired, but don't crash
                    print(f"Exception encountered: {data}")
                    continue
                if data is not None and "results" in data:
                    # Optionally filter results as they come in
                    current_results = data["results"]
                    if filter and callable(filter):
                        current_results = filter(current_results)

                    all_results.extend(current_results)

                    next_url = data.get("next_url")
                    if next_url:
                        # Make sure to append the API key in the next_url
                        # If the original url had the apiKey, ensure next_url also has it.
                        # If not included in next_url by the API, we add it again:
                        if "apiKey=" not in next_url:
                            next_url += f'&{urlencode({"apiKey": self.api_key})}'
                        pages_to_fetch.append(next_url)

        if as_dataframe:
            return pd.DataFrame(all_results)
        return all_results
        

    async def fetch_endpoint(self, url):
        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            return data.json()

    async def last_trade(self, ticker):
        """Gets the last trade details for a ticker."""
        endpoint = f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={self.api_key}"


        try:
            async with httpx.AsyncClient() as client:
                data = await client.get(endpoint)
                data = data.json()
                results = data.get('results')

                if results:
                    return LastTradeData(results)
                else:
                    print("No results found")
        except aiohttp.ClientResponseError as e:
            print(f"Client response error - status {e.status}: {e.message}")
        except aiohttp.ClientError as e:
            print(f"Client error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        


    
    async def daily_open_close(self, ticker:str, date:str):
        """Gets the daily open/close for a ticker and a date."""

        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={self.api_key}")
            data = data.json()
    
            return DailyOpenClose(data)
                    

    async def get_aggs(self, ticker:str='AAPL', multiplier:int=1, timespan:str='second', date_from:str='2024-01-01', date_to:str='2024-04-12', adjusted:str='true', sort:str='asc', limit:int=50000):
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


        endpoint = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}?adjusted={adjusted}&sort={sort}&limit={limit}&apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
  
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)

            data = data.json()

            results = data['results'] if 'results' in data else None

            if results is not None:

                results = Aggregates(results, ticker)


                return results

    async def fetch(self, url):

        async with httpx.AsyncClient() as client:
            resp = await client.get(url)

            if resp.status_code == 200:
                resp = resp.json()
                return resp
    async def fetch_realtime_price(self,ticker, multiplier:int=1, timespan:str='second', date_from:str=today, date_to:str=today):
        if ticker in self.indices_list:
            ticker = f"I:{ticker}"
        url=f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}?sort=desc&apiKey={os.environ.get('YOUR_POLYGON_KEY')}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)

            if data.status_code == 200:
                data = data.json()

                results = data['results']
                close = [i.get('c') for i in results]
                close = close[0]

                return close
            
    async def calculate_support_resistance(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = "hour",
        date_from: str = None,  # Default to None, can set defaults elsewhere
        date_to: str = None,  # Default to None, can set defaults elsewhere
        levels: int = 1,  # Number of additional resistance/support levels to calculate
    ):
        """
        Calculate pivot points, support, and resistance levels for a given stock ticker.

        Args:
            ticker (str): Stock ticker symbol.
            multiplier (int): Multiplier for aggregation. Defaults to 1.
            timespan (str): Timespan for data aggregation (e.g., 'hour', 'day'). Defaults to 'hour'.
            date_from (str): Start date for fetching data (format: 'YYYY-MM-DD'). Defaults to None.
            date_to (str): End date for fetching data (format: 'YYYY-MM-DD'). Defaults to None.
            levels (int): Number of additional support and resistance levels to calculate. Defaults to 2.

        Returns:
            pd.DataFrame: DataFrame containing support, resistance, pivot levels, and stock price.
        """
        try:
            if date_from == None:
                date_from = eight_days_ago
            if date_to == None:
                date_to = today
            # Fetch aggregate data
            aggs = await self.get_aggs(ticker=ticker, multiplier=multiplier, timespan=timespan, date_from=date_from, date_to=date_to)
            df = aggs.as_dataframe
            
            # Ensure the DataFrame has required columns
            required_columns = {"high", "low", "close"}
            if not required_columns.issubset(df.columns):
                raise ValueError(f"Missing required columns in data: {required_columns - set(df.columns)}")
            
            # Calculate pivot point
            df["pivot_point"] = (df["high"] + df["low"] + df["close"]) / 3

            # Calculate primary support and resistance levels
            df["resistance_1"] = 2 * df["pivot_point"] - df["low"]
            df["support_1"] = 2 * df["pivot_point"] - df["high"]

            # Calculate additional support and resistance levels if specified
            for level in range(2, levels + 1):
                df[f"resistance_{level}"] = df["pivot_point"] + (df["high"] - df["low"]) * level
                df[f"support_{level}"] = df["pivot_point"] - (df["high"] - df["low"]) * level
            
            # Include stock price for reference
            df["stock_price"] = df["close"]

            # Select only relevant columns
            columns_to_return = ["pivot_point", "stock_price"] + \
                                [f"resistance_{i}" for i in range(1, levels + 1)] + \
                                [f"support_{i}" for i in range(1, levels + 1)]
            
            result_df = df[columns_to_return]
            result_df['ticker'] = ticker
            result_df['timespan'] = timespan

            return result_df[::-1].iloc[0]

        except Exception as e:
            raise RuntimeError(f"Error calculating support and resistance levels: {e}")


    async def ema_check(self, ticker, ema_lengths):
        """Checks jawless emas
        
        21/55/144 returns TRUE or FALSE if the EMAs are either above or below the current price."""
     
        # Define EMA lengths and corresponding URLs

        urls = [
            f"https://api.polygon.io/v1/indicators/ema/{ticker}?timespan=day&adjusted=true&window={length}&series_type=close&order=desc&apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
            for length in ema_lengths
        ]

        # Fetch EMA data concurrently
        tasks = [self.fetch(url) for url in urls]
        data = await asyncio.gather(*tasks)

        # Extract results and values, pairing them with EMA lengths
        results = [i.get('results') for i in data]

        # Convert EMA data to a DataFrame
        values_per_ema = [
            pd.DataFrame(
                {
                    "EMA Length": [length] * len(result.get('values', [])),
                    "Date": [datetime.fromtimestamp(v['timestamp'] / 1000).strftime('%Y-%m-%d') for v in result.get('values', [])],
                    "Value": [v['value'] for v in result.get('values', [])]
                }
            )
            for length, result in zip(ema_lengths, results)
        ]

        # Concatenate all DataFrames into a single DataFrame
        df = pd.concat(values_per_ema, ignore_index=True)
        df['ticker'] = ticker

        # Get the latest EMA values
        latest_ema_values = df.sort_values('Date').groupby('EMA Length').tail(1).reset_index(drop=True)

        # Fetch the current price
        price = await self.get_price(ticker)

        # Compare each EMA value with the current price
        above_current_price = all(latest_ema_values['Value'] > price)
        below_current_price = all(latest_ema_values['Value'] < price)

        # Store the result as TRUE or FALSE string
        all_above = "TRUE" if above_current_price else "FALSE"
        all_below = "TRUE" if below_current_price else "FALSE"

        df['above'] = all_above
        df['below'] = all_below

        return df
    async def market_news(self, limit: str = '100'):
        """
        Arguments:

        >>> ticker: the ticker to query (optional)
        >>> limit: the number of news items to return (optional) | Max 1000

        """


        endpoint = f"https://api.polygon.io/v2/reference/news?limit={limit}&apiKey={os.environ.get('YOUR_POLYGON_KEY')}"

        data = await self.fetch_endpoint(endpoint)
        results = data['results']
        data = TickerNews(results)

        return data
    

    async def dark_pools(self, ticker:str, multiplier:int, timespan:str, date_from:str, date_to:str):

        aggs = await self.get_aggs(ticker=ticker, multiplier=multiplier, timespan=timespan, date_from=date_from, date_to=date_to)



        # Assuming 'aggs' is an instance of the Aggregates class with populated data
        dollar_cost_above_10m_details = [
            {'Close Price': aggs.close[i], 'Timestamp': aggs.timestamp[i], 'Dollar Cost': cost}
            for i, cost in enumerate(aggs.dollar_cost) 
            if cost > 10000000
        ]

        # Create DataFrame from the list of dictionaries
        df_dollar_cost_above_10m = pd.DataFrame(dollar_cost_above_10m_details)

        # Print the DataFrame to see the result
        df = format_large_numbers_in_dataframe(df_dollar_cost_above_10m)

        return df

    async def top_gainers_losers(self, type:str='gainers'):
        """Fetches the top gainers/losers on the day."""
        endpoint = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/{type}?apiKey={self.api_key}"

        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)
            data = data.json()
            tickers = data['tickers'] if 'tickers' in data else None
            return GainersLosers(tickers)

    async def company_info(self, ticker):
        """Gets company information for a ticker."""
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"

        try:
            async with httpx.AsyncClient() as client:
                data = await client.get(url)
                data = data.json()
                results = data['results']
                return CompanyResults(results)
        except Exception as e:
            print(e)

  
    async def get_all_tickers(self, include_otc=False, save_all_tickers:bool=False):
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
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={self.api_key}&include_otc={include_otc}"


        try:
            async with httpx.AsyncClient() as client:
                data = await client.get(url)
                response_data = data.json()



                tickers = response_data['tickers']
                
                data = StockSnapshot(tickers)

                return data
        except Exception as e:
            print(e)

    async def stock_snapshot(self, ticker:str):

        """
        Get a stock's snapshot!
        
        """

        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={self.api_key}")
            data = data.json()
            ticker = data['ticker']

            return SingleStockSnapshot(ticker)

    # async def rsi(self, ticker:str, timespan:str, limit:str='1', window:int=14, date_from:str=None, date_to:str=None, session=None, snapshot:bool=False):
    #     """
    #     Arguments:

    #     >>> ticker

    #     >>> AVAILABLE TIMESPANS:

    #     minute
    #     hour
    #     day
    #     week
    #     month
    #     quarter
    #     year

    #     >>> date_from (optional) 
    #     >>> date_to (optional)
    #     >>> window: the RSI window (default 14)
    #     >>> limit: the number of N timespans to survey
        
    #     >>> *SNAPSHOT: scan all timeframes for a ticker

    #     """
    #     try:
    #         if date_from is None:
    #             date_from = self.eight_days_ago

    #         if date_to is None:
    #             date_to = self.today

    #         if timespan == 'month':
    #             date_from = self.thirty_days_ago
            
    #         endpoint = f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan={timespan}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&window={window}&expand_underlying=true&apiKey={self.api_key}"
    
       
    #         async with httpx.AsyncClient() as client:
    #             try:
    #                 data = await client.get(endpoint)
    #                 datas = data.json()
                      
    #                 if datas is not None:

                        

                
    #                     return RSI(datas, ticker)
    #             except ClientOSError as e:
    #                 print(f'ERROR {e}')
                

    #         if snapshot == True:
    #             tasks = []
    #             timespans = self.timeframes
    #             for timespan in timespans:
    #                 tasks.append(asyncio.create_task)
    #     except Exception as e:
    #         print(e)


    # async def get_cik(self, ticker):
    #     """Get a CIK number for a ticker."""
    #     try:
    #         endpoint = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
    #         async with httpx.AsyncClient() as client:
    #             data = await client.get(endpoint)

    #             cik = data.json()['results']['cik']

                
    #             return cik
    #     except Exception as e:
    #         print(e)

    async def macd(self, ticker: str, timespan: str, limit: str = '1000'):
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
        >>> window: the RSI window (default 14)
        >>> limit: the number of N timespans to survey
        
        """
        try:
            endpoint = f"https://api.polygon.io/v1/indicators/macd/{ticker}?timespan={timespan}&adjusted=true&short_window=12&long_window=26&signal_window=9&series_type=close&order=desc&apiKey={self.api_key}&limit={limit}"
            async with httpx.AsyncClient() as client:
                data = await client.get(endpoint)
                datas = data.json()
                if datas is not None:
                    return MACD(datas, ticker)
        except Exception as e:
            print(f"Unexpected error - {ticker}: {e}")
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
        try:
            if date_from is None:
                date_from = self.eight_days_ago

            if date_to is None:
                date_to = self.today


            endpoint = f"https://api.polygon.io/v1/indicators/sma/{ticker}?timespan={timespan}&window={window}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&apiKey={self.api_key}"
            await self.create_session()
            try:
                

                async with self.session.get(endpoint) as resp:
                    datas = await resp.json()


                    return SMA(datas, ticker)
            finally:
                pass
        except Exception as e:
            print(e)


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
        try:
            if date_from is None:
                date_from = self.eight_days_ago

            if date_to is None:
                date_to = self.today


            endpoint = f"https://api.polygon.io/v1/indicators/ema/{ticker}?timespan={timespan}&window={window}&timestamp.gte={date_from}&timestamp.lte={date_to}&limit={limit}&apiKey={self.api_key}"

            
            try:
                await self.create_session()  # Ensure the session is created
                async with self.session.get(endpoint) as resp:
                    datas = await resp.json()
                    return EMA(datas, ticker)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)





    async def get_price(self, ticker):
        """Get the price of a ticker, index, option, or crypto coin"""
        try:
            if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX']:
                ticker = f"I:{ticker}"
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
          
            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                if r.status_code == 200:
                    r = r.json()
                    results = r['results'] if 'results' in r else None
                    if results is not None:
                        session = [i.get('session') for i in results]
                        price = [i.get('close') for i in session]
                        return price[0]
        except Exception as e:
            print(f"{ticker} ... {e}")

    async def rsi_snapshot(self, ticker) -> pd.DataFrame:
        """Gather a snapshot of the RSI across the minute, day, hour ,week, and month timespans."""
        try:

            timespans = ['minute', 'day', 'hour', 'week', 'month']
            # Create tasks for RSI computations
            rsi_tasks = {timespan: asyncio.create_task(self.rsi(ticker, timespan=timespan)) for timespan in timespans}

            # Wait for all tasks to complete
            # Use return_exceptions=True to handle individual task exceptions
            results = await asyncio.gather( 
                *rsi_tasks.values(), 
                return_exceptions=True
            )

            # Separate the price and RSI results
            price = results[0]
            rsi_results = dict(zip(timespans, results[1:]))

            # Check if price was retrieved successfully
            if price is None or isinstance(price, Exception):
                print(f"Failed to fetch price for ticker {ticker}: {price}")
                return None

            # Initialize the result dictionary
            rsis = {'ticker': ticker, 'price': price}

            # Process RSI results
            for timespan, rsi_result in rsi_results.items():
                # Check if the result is an exception
                if isinstance(rsi_result, Exception):
                    print(f"Error fetching RSI for {ticker} at timespan '{timespan}': {rsi_result}")
                    continue

                # Safely extract the RSI value
                rsi_value = self.extract_rsi_value(rsi_result)
                if rsi_value is not None:
                    rsis[f"{timespan}_rsi"] = rsi_value
                else:
                    print(f"No RSI data for ticker {ticker} at timespan '{timespan}'")

            # Create a DataFrame if we have RSI data
            if len(rsis) > 2:  # more than just 'ticker' and 'price'
                df = pd.DataFrame([rsis])
                return df
            else:
                print(f"No RSI data available for ticker {ticker}")
                return None

        except Exception as e:
            print(f"Exception in rsi_snapshot for ticker {ticker}: {e}")
            return None

    def extract_rsi_value(self, rsi_data):
        """Helper method to extract RSI value safely."""
        try:
            if rsi_data and hasattr(rsi_data, 'rsi_value') and rsi_data.rsi_value:
                return rsi_data.rsi_value[0]
        except Exception as e:
            print(f"Error extracting RSI value: {e}")
        return None
    async def check_macd_sentiment(self, hist: list):
        try:
            if hist is not None:
                if hist is not None and len(hist) >= 3:
                    
                    last_three_values = hist[:3]
                    if abs(last_three_values[0] - (-0.02)) < 0.04 and all(last_three_values[i] > last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                        return 'bullish'

                    if abs(last_three_values[0] - 0.02) < 0.04 and all(last_three_values[i] < last_three_values[i + 1] for i in range(len(last_three_values) - 1)):
                        return 'bearish'
                else:
                    return '-'
        except Exception as e:
            print(e)

    async def histogram_snapshot(self, ticker) -> pd.DataFrame:
        """Returns a dataframe of bullish/bearish iminent MACD cross if detected."""
        try:


            # Create MACD tasks for different timespans
            timespans = ['day', 'hour', 'week', 'month']
            macd_tasks = {
                timespan: asyncio.create_task(self.macd(ticker, timespan=timespan, limit='10'))
                for timespan in timespans
            }

            # Wait for all tasks to complete
            results = await asyncio.gather(
                *macd_tasks.values(),
                return_exceptions=True
            )


            macd_results = dict(zip(timespans, results))



            histograms = {}
            sentiment_tasks = {}

            # Process MACD results and start sentiment analysis tasks concurrently
            for timespan, macd_result in macd_results.items():
                if isinstance(macd_result, Exception):
                    print(f"Error fetching MACD for {ticker} at timespan '{timespan}': {macd_result}")
                    continue

                if (
                    macd_result is not None and
                    hasattr(macd_result, 'macd_histogram') and
                    macd_result.macd_histogram is not None and
                    len(macd_result.macd_histogram) > 2
                ):
                    hist = macd_result.macd_histogram
                    # Start check_macd_sentiment task
                    sentiment_tasks[timespan] = asyncio.create_task(self.check_macd_sentiment(hist))
                else:
                    print(f"Invalid MACD histogram data for {ticker} at timespan '{timespan}'.")

            # Await sentiment analysis tasks concurrently
            if sentiment_tasks:
                sentiment_results = await asyncio.gather(
                    *sentiment_tasks.values(),
                    return_exceptions=True
                )

                for timespan, sentiment in zip(sentiment_tasks.keys(), sentiment_results):
                    if isinstance(sentiment, Exception):
                        print(f"Error in check_macd_sentiment for {ticker} at timespan '{timespan}': {sentiment}")
                    else:
                        histograms[f"{timespan}_macd"] = sentiment

            # Build DataFrame
            df = pd.DataFrame(histograms, index=[0])
            df['ticker'] = ticker


            return df
        except Exception as e:
            print(f"Exception in histogram_snapshot for ticker {ticker}: {e}")
            return None

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
        try:
            url = f'https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={self.api_key}'
            try:
                async with httpx.AsyncClient() as client:
                    data = await client.get(url)

                    data = data.json()
                    
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

            finally:
                pass
        except Exception as e:
            print(e)
    async def stock_trades(self, ticker: str, limit: str = '50000', timestamp_gte: str = None, timestamp_lte: str = None):
        """
        Get trades for a ticker. Limit defaults to 50,000 results.
        Timestamp defaults to thirty days ago.
        """
        if timestamp_gte is None:
            timestamp_gte = self.thirty_days_ago
        if timestamp_lte is None:
            timestamp_lte = self.today

        # Construct the initial endpoint
        endpoint = (
            f"https://api.polygon.io/v3/trades/{ticker}"
            f"?timestamp.gte={timestamp_gte}"
            f"&timestamp.lte={timestamp_lte}"
            f"&apiKey={self.api_key}"
            f"&limit={limit}"
        )

        data = await self.paginate_concurrent(endpoint, as_dataframe=False)

        # Assuming TradeData is a class you have defined elsewhere
        # that wraps the list of trades and includes metadata like ticker.
        return TradeData(data, ticker=ticker)
            


    async def get_prices(self, tickers):
        """Get the prices of multiple tickers in a single API call."""
        try:
            # Prepare the tickers string for the API call
            tickers = [f"I:{ticker}" if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX'] else ticker for ticker in tickers]
            tickers_str = ",".join(tickers)
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={tickers_str}&apiKey={self.api_key}"
            await self.create_session()
            response = await self.session.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            prices = {}
            for item in results:
                ticker = item.get('ticker')
                session_data = item.get('session', {})
                close_price = session_data.get('close')
                prices[ticker] = close_price
            return prices
        except Exception as e:
            print(f"Error fetching prices: {e}")
            return {}



    async def rsi(self, tickers: List[str], timespan: str, limit: str = '1', window: int = 14, date_from: str = None, date_to: str = None):
        """
        Fetch RSI data for a list of tickers for a specific timespan.
        """
        if date_from is None:
            date_from = self.eight_days_ago

        if date_to is None:
            date_to = self.today

        if timespan == 'month':
            date_from = self.thirty_days_ago

        tasks = []
        for ticker in tickers:
            endpoint = (
                f"https://api.polygon.io/v1/indicators/rsi/{ticker}"
                f"?timespan={timespan}&timestamp.gte={date_from}&timestamp.lte={date_to}"
                f"&limit={limit}&window={window}&expand_underlying=true&apiKey={self.api_key}"
            )
            tasks.append(self.fetch_rsi_data(endpoint, ticker))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        return {ticker: result for ticker, result in results if result is not None}

    async def fetch_rsi_data(self, endpoint: str, ticker: str):
        try:
            await self.create_session()
            response = await self.session.get(endpoint)
            response.raise_for_status()
            data = response.json()
            return ticker, data  # Return a tuple of ticker and data
        except Exception as e:
            print(f"Error fetching RSI data for {ticker}: {e}")
            return ticker, None

    def extract_rsi_value(self, rsi_data):
        """Helper method to extract the most recent RSI value safely."""
        try:
            if rsi_data and 'results' in rsi_data:
                values = rsi_data['results'].get('values')
                if values and len(values) > 0:
                    return values[-1]['value']  # Get the latest RSI value
        except Exception as e:
            print(f"Error extracting RSI value: {e}")
        return None
    async def rsi_snapshot(self, tickers: List[str]) -> pd.DataFrame:
        """
        Gather a snapshot of the RSI across multiple timespans for multiple tickers.
        """
        timespans = ['minute', 'day', 'hour', 'week', 'month']
        tasks = []
        for timespan in timespans:
            tasks.append(self.rsi(tickers, timespan))

        # Run RSI calculations concurrently for all timespans
        rsi_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate the results
        aggregated_data = {}
        for timespan, rsi_data in zip(timespans, rsi_results):
            if isinstance(rsi_data, Exception):
                print(f"Error fetching RSI data for timespan '{timespan}': {rsi_data}")
                continue
            for ticker, data in rsi_data.items():
                if ticker not in aggregated_data:
                    aggregated_data[ticker] = {}
                rsi_value = self.extract_rsi_value(data)
                if rsi_value is not None:
                    aggregated_data[ticker][f"{timespan}_rsi"] = rsi_value

        # Convert aggregated data to DataFrame
        records = []
        for ticker, rsi_values in aggregated_data.items():
            record = {'ticker': ticker}
            record.update(rsi_values)
            if len(rsi_values) > 0:
                records.append(record)

        if records:
            df = pd.DataFrame(records)
            return df
        else:
            print("No RSI data available for the provided tickers.")
            return None



    async def stock_snapshots(self, tickers: List[str]) -> pd.DataFrame:
        """
        Fetch snapshots for multiple tickers and return a DataFrame.
        """
        try:
            await self.create_session()
            url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={self.api_key}"
            response = await self.session.get(url)
            response.raise_for_status()
            data = response.json()

            # Extract snapshots
            snapshots = data.get('tickers', [])
            return StockSnapshot(snapshots).as_dataframe
        except Exception as e:
            print(f"Error fetching stock snapshots: {e}")
            return None