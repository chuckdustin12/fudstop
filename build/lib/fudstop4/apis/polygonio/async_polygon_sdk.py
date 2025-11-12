import sys
from pathlib import Path
import os
from datetime import datetime, timedelta
import asyncio
import logging
from typing import List, Dict, Optional
from typing import Dict, Tuple, Callable, Union, Any
# Third-party imports
import httpx
import requests
import pandas as pd
from dotenv import load_dotenv
import aiohttp
from asyncpg import create_pool
from asyncpg.exceptions import UniqueViolationError
from urllib.parse import unquote, urlencode
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError, ClientConnectionError, ContentTypeError

# Local package imports
from fudstop4.apis.helpers import format_large_numbers_in_dataframe, flatten_dict
from .models.company_info import CompanyResults
from .models.quotes import StockQuotes, LastStockQuote
from .models.aggregates import Aggregates
from .models.ticker_news import TickerNews
from .models.technicals import RSI, EMA, SMA, MACD
from .models.gainers_losers import GainersLosers
from .models.ticker_snapshot import StockSnapshot, SingleStockSnapshot
from .models.trades import TradeData, LastTradeData
from .models.daily_open_close import DailyOpenClose

# Load environment variables from .env file
load_dotenv()

# Setup module-level logger
logger = logging.getLogger(__name__)

# Add the project directory to sys.path if not already present
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

# Global date variables (retained for compatibility)
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

# Global requests session (retained; may be used in other sections)
session = requests.session()


class Polygon:
    """
    A client for interacting with the Polygon API and a PostgreSQL database.
    This class provides methods to fetch market data, manage an asynchronous HTTP session,
    and interact with the database using an asyncpg connection pool.
    """
    def __init__(
        self,
        host: str = 'localhost',
        user: str = 'chuck',
        database: str = 'fudstop3',
        password: str = 'fud',
        port: int = 5432
    ) -> None:
        self.host: str = host
        self.indices_list: List[str] = ['NDX', 'RUT', 'SPX', 'VIX', 'XSP']
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.database: str = database
        self.api_key: Optional[str] = os.environ.get('YOUR_POLYGON_KEY')
        self.today: str = datetime.now().strftime('%Y-%m-%d')
        self.yesterday: str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow: str = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago: str = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now: str = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago: str = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now: str = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now: str = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago: str = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.timeframes: List[str] = ['minute', 'hour', 'day', 'week', 'month']
        self.session: Optional[httpx.AsyncClient] = None
        self.pool = None  # This will be set when connect() is called

    async def create_session(self) -> None:
        """
        Creates an asynchronous HTTP session using httpx.AsyncClient.
        Reuses the same session if it already exists and is open.
        """
        if self.session is None or self.session.is_closed:
            logger.debug("Creating new httpx AsyncClient session.")
            self.session = httpx.AsyncClient(http2=True, timeout=30.0)

    async def close_session(self) -> None:
        """
        Closes the current asynchronous HTTP session if it is open.
        """
        if self.session is not None and not self.session.is_closed:
            logger.debug("Closing the httpx AsyncClient session.")
            await self.session.aclose()
            self.session = None

    async def get_prices(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        """
        Fetches prices for multiple tickers in a single API call.
        
        Parameters:
            tickers (List[str]): List of ticker symbols.
        
        Returns:
            Dict[str, Optional[float]]: Mapping of ticker symbols to their close price.
        """
        try:
            # Adjust ticker symbols for indices that require a prefix
            tickers = [
                f"I:{ticker}" if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX'] else ticker
                for ticker in tickers
            ]
            tickers_str = ",".join(tickers)
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={tickers_str}&apiKey={self.api_key}"
            await self.create_session()
            response = await self.session.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            prices: Dict[str, Optional[float]] = {}
            for item in results:
                ticker_symbol = item.get('ticker')
                session_data = item.get('session', {})
                close_price = session_data.get('close')
                prices[ticker_symbol] = close_price
            return prices
        except Exception as e:
            logger.error(f"Error fetching prices: {e}", exc_info=True)
            return {}

    async def fetch_endpoint(self, url: str) -> dict:
        """
        Fetches JSON data from a given API endpoint.
        
        Parameters:
            url (str): The API endpoint URL.
        
        Returns:
            dict: The JSON response.
        """
        await self.create_session()  # Ensure session is created
        async with self.session.get(url) as response:
            response.raise_for_status()  # Raises exception for HTTP errors
            return await response.json()

    async def connect(self, connection_string: Optional[str] = None):
        """
        Establishes a connection pool to the PostgreSQL database.
        
        Parameters:
            connection_string (Optional[str]): Optional custom connection string.
        
        Returns:
            The created asyncpg pool.
        """
        if connection_string:
            self.pool = await create_pool(
                host=self.host,
                database=self.database,
                password=self.password,
                user=self.user,
                port=self.port,
                min_size=1,
                max_size=30
            )
        else:
            self.pool = await create_pool(
                host=os.environ.get('DB_HOST'),
                port=os.environ.get('DB_PORT'),
                user=os.environ.get('DB_USER'),
                password=os.environ.get('DB_PASSWORD'),
                database='fudstop3',
                min_size=1,
                max_size=10
            )
        logger.debug("Database connection pool created.")
        return self.pool

    async def save_structured_message(self, data: dict, table_name: str) -> None:
        """
        Saves a structured message (dictionary of values) into a specified database table.
        
        Parameters:
            data (dict): The data to be inserted.
            table_name (str): The name of the table where data will be inserted.
        """
        fields = ', '.join(data.keys())
        values = ', '.join([f"${i+1}" for i in range(len(data))])
        query = f'INSERT INTO {table_name} ({fields}) VALUES ({values})'
      
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(query, *data.values())
                logger.debug("Data inserted into table '%s'.", table_name)
            except UniqueViolationError:
                logger.warning("Duplicate entry detected for table '%s' - skipping insertion.", table_name)




    async def fetch_page(self, url: str) -> Optional[dict]:
        """
        Fetch a single page of data from the provided URL.

        Parameters:
            url (str): The URL to fetch data from.

        Returns:
            Optional[dict]: Parsed JSON response if successful; otherwise, None.
        """
        if self.session is None:
            await self.create_session()

        try:
            response = await self.session.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error("Error fetching %s: %s", url, e, exc_info=True)
            return None

    async def stock_quotes(self, ticker: str, limit: str = '50000') -> Optional[StockQuotes]:
        """
        Gets stock quotes. Default limit is 50000.

        Parameters:
            ticker (str): The ticker symbol.
            limit (str): The maximum number of quotes to retrieve.

        Returns:
            Optional[StockQuotes]: Parsed stock quotes data.
        """
        url = f"https://api.polygon.io/v3/quotes/{ticker}?limit={limit}&apiKey={self.api_key}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                return StockQuotes(results)
        except httpx.HTTPError as e:
            logger.error("Error fetching stock quotes for %s: %s", ticker, e, exc_info=True)
            return None

    async def last_stock_quote(self, ticker: str) -> Optional[LastStockQuote]:
        """
        Get the latest quote for a ticker.

        Parameters:
            ticker (str): The ticker symbol.

        Returns:
            Optional[LastStockQuote]: The latest stock quote, or None if not available.
        """
        url = f"https://api.polygon.io/v2/last/nbbo/{ticker}?apiKey={self.api_key}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                results = data.get('results')
                if results is None:
                    logger.warning("No results in last_stock_quote for ticker: %s", ticker)
                    return None
                return LastStockQuote(results)
        except httpx.HTTPError as e:
            logger.error("Error fetching last stock quote for %s: %s", ticker, e, exc_info=True)
            return None

    async def paginate_concurrent(
        self,
        url: str,
        as_dataframe: bool = False,
        concurrency: int = 250,
        filter: Optional[Callable[[List[Any]], List[Any]]] = None,
    ) -> Union[pd.DataFrame, List[Any]]:
        """
        Paginate through multiple pages concurrently from a Polygon.io endpoint.

        Parameters:
            url (str): Starting URL with an apiKey included.
            as_dataframe (bool): If True, return results as a Pandas DataFrame.
            concurrency (int): Maximum number of concurrent requests.
            filter (Optional[Callable[[List[Any]], List[Any]]]): Optional function to filter results.

        Returns:
            Union[pd.DataFrame, List[Any]]: Combined results either as a DataFrame or a list.
        """
        await self.create_session()
        all_results: List[Any] = []
        pages_to_fetch: List[str] = [url]
        sem = asyncio.Semaphore(concurrency)

        async def fetch_with_sem(page_url: str) -> Optional[dict]:
            async with sem:
                return await self.fetch_page(page_url)

        while pages_to_fetch:
            tasks = [fetch_with_sem(page_url) for page_url in pages_to_fetch]
            pages_to_fetch = []
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for data in results:
                if isinstance(data, Exception):
                    logger.error("Exception encountered during pagination: %s", data, exc_info=True)
                    continue
                if data is not None and "results" in data:
                    current_results = data["results"]
                    if filter is not None and callable(filter):
                        try:
                            current_results = filter(current_results)
                        except Exception as e:
                            logger.error("Error applying filter function: %s", e, exc_info=True)
                    all_results.extend(current_results)
                    next_url = data.get("next_url")
                    if next_url:
                        # Append apiKey if missing in the next_url.
                        if "apiKey=" not in next_url:
                            next_url += f'&{urlencode({"apiKey": self.api_key})}'
                        pages_to_fetch.append(next_url)
        if as_dataframe:
            return pd.DataFrame(all_results)
        return all_results

    async def fetch_endpoint(self, url: str) -> dict:
        """
        Fetch JSON data from a given API endpoint.

        Parameters:
            url (str): The URL to fetch data from.

        Returns:
            dict: Parsed JSON response or an empty dict on error.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()
        except httpx.HTTPError as e:
            logger.error("Error fetching endpoint %s: %s", url, e, exc_info=True)
            return {}

    async def last_trade(self, ticker: str) -> Optional[LastTradeData]:
        """
        Gets the last trade details for a ticker.

        Parameters:
            ticker (str): The ticker symbol.

        Returns:
            Optional[LastTradeData]: The last trade data or None if unavailable.
        """
        endpoint = f"https://api.polygon.io/v2/last/trade/{ticker}?apiKey={self.api_key}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                data = response.json()
                results = data.get('results')
                if results:
                    return LastTradeData(results)
                else:
                    logger.warning("No results found for last_trade with ticker: %s", ticker)
                    return None
        except httpx.HTTPError as e:
            logger.error("HTTP error in last_trade for %s: %s", ticker, e, exc_info=True)
            return None
        except Exception as e:
            logger.error("Unexpected error in last_trade for %s: %s", ticker, e, exc_info=True)
            return None

    async def daily_open_close(self, ticker: str, date: str) -> Optional[DailyOpenClose]:
        """
        Gets the daily open/close for a ticker on a given date.

        Parameters:
            ticker (str): The ticker symbol.
            date (str): The date in yyyy-mm-dd format.

        Returns:
            Optional[DailyOpenClose]: Daily open/close data or None on error.
        """
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={self.api_key}"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                return DailyOpenClose(data)
        except httpx.HTTPError as e:
            logger.error("Error fetching daily open/close for %s on %s: %s", ticker, date, e, exc_info=True)
            return None

    async def get_aggs(
        self,
        ticker: str = 'AAPL',
        multiplier: int = 1,
        timespan: str = 'second',
        date_from: str = '2024-01-01',
        date_to: str = '2024-04-12',
        adjusted: str = 'true',
        sort: str = 'asc',
        limit: int = 50000,
    ) -> Optional[Aggregates]:
        """
        Fetches candlestick (aggregation) data for a ticker.

        Parameters:
            ticker (str): The ticker symbol.
            multiplier (int): The number of timespans to aggregate.
            timespan (str): The timespan for each aggregation.
            date_from (str): Start date (yyyy-mm-dd).
            date_to (str): End date (yyyy-mm-dd).
            adjusted (str): Whether the data is adjusted.
            sort (str): Sort order (asc/desc).
            limit (int): Maximum number of candles to return.

        Returns:
            Optional[Aggregates]: Aggregated data or None if not found.
        """
        endpoint = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/"
            f"{date_from}/{date_to}?adjusted={adjusted}&sort={sort}&limit={limit}&apiKey={self.api_key}"
        )
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                data = response.json()
                results = data.get('results')
                if results is not None:
                    return Aggregates(results, ticker)
                else:
                    logger.warning("No aggregation results found for ticker: %s", ticker)
                    return None
        except httpx.HTTPError as e:
            logger.error("Error fetching aggregations for %s: %s", ticker, e, exc_info=True)
            return None

    async def fetch(self, url: str) -> Optional[dict]:
        """
        Fetch data from the given URL.

        Parameters:
            url (str): The URL to fetch.

        Returns:
            Optional[dict]: Parsed JSON data if status is 200; otherwise, None.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning("Non-200 response (%s) for URL: %s", response.status_code, url)
                    return None
        except httpx.HTTPError as e:
            logger.error("Error in fetch for URL %s: %s", url, e, exc_info=True)
            return None

    async def fetch_realtime_price(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = 'second',
        date_from: str = today,
        date_to: str = today,
    ) -> Optional[float]:
        """
        Fetch the real-time price for a given ticker.

        Parameters:
            ticker (str): The ticker symbol.
            multiplier (int): Aggregation multiplier.
            timespan (str): Aggregation timespan.
            date_from (str): Start date (default is today's date).
            date_to (str): End date (default is today's date).

        Returns:
            Optional[float]: The latest close price, or None if unavailable.
        """
        if ticker in self.indices_list:
            ticker = f"I:{ticker}"
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/"
            f"{date_from}/{date_to}?sort=desc&apiKey={self.api_key}"
        )
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                if results:
                    close_prices = [item.get('c') for item in results if 'c' in item]
                    if close_prices:
                        return close_prices[0]
                    else:
                        logger.warning("No close prices found in results for ticker: %s", ticker)
                        return None
                else:
                    logger.warning("No results found for realtime price for ticker: %s", ticker)
                    return None
        except httpx.HTTPError as e:
            logger.error("Error fetching realtime price for %s: %s", ticker, e, exc_info=True)
            return None
    async def calculate_support_resistance(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = "hour",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        levels: int = 1,
    ) -> pd.Series:
        """
        Calculate pivot points, support, and resistance levels for a given stock ticker.

        Args:
            ticker (str): Stock ticker symbol.
            multiplier (int): Multiplier for aggregation. Defaults to 1.
            timespan (str): Timespan for data aggregation (e.g., 'hour', 'day'). Defaults to 'hour'.
            date_from (Optional[str]): Start date for data fetch (format: 'YYYY-MM-DD'). If None, defaults to eight days ago.
            date_to (Optional[str]): End date for data fetch (format: 'YYYY-MM-DD'). If None, defaults to today.
            levels (int): Number of additional support/resistance levels to calculate. Defaults to 1.

        Returns:
            pd.Series: A series containing pivot point, support/resistance levels, stock price, and timespan.
        """
        try:
            if date_from is None:
                date_from = self.eight_days_ago  # Assuming self.eight_days_ago is set in __init__
            if date_to is None:
                date_to = self.today  # Assuming self.today is set in __init__

            # Fetch aggregate data
            aggs = await self.get_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                date_from=date_from,
                date_to=date_to,
            )
            if not aggs:
                raise ValueError("No aggregate data returned.")

            df = aggs.as_dataframe

            # Ensure the DataFrame has required columns
            required_columns = {"high", "low", "close"}
            if not required_columns.issubset(df.columns):
                missing = required_columns - set(df.columns)
                raise ValueError(f"Missing required columns in data: {missing}")

            # Calculate pivot point, primary resistance and support
            df["pivot_point"] = (df["high"] + df["low"] + df["close"]) / 3
            df["resistance_1"] = 2 * df["pivot_point"] - df["low"]
            df["support_1"] = 2 * df["pivot_point"] - df["high"]

            # Calculate additional support and resistance levels if specified
            for level in range(2, levels + 1):
                df[f"resistance_{level}"] = df["pivot_point"] + (df["high"] - df["low"]) * level
                df[f"support_{level}"] = df["pivot_point"] - (df["high"] - df["low"]) * level

            # Include stock price for reference and add ticker column
            df["stock_price"] = df["close"]
            df["ticker"] = ticker

            # Select only relevant columns and include timespan
            columns_to_return = ["pivot_point", "stock_price"] + \
                                [f"resistance_{i}" for i in range(1, levels + 1)] + \
                                [f"support_{i}" for i in range(1, levels + 1)]
            result_df = df[columns_to_return]
            result_df["timespan"] = timespan

            # Return the most recent row (reversed DataFrame)
            return result_df[::-1].iloc[0]

        except Exception as e:
            logger.error("Error calculating support and resistance levels for %s: %s", ticker, e, exc_info=True)
            raise RuntimeError(f"Error calculating support and resistance levels: {e}")

    async def ema_check(self, ticker: str, ema_lengths: List[int]) -> pd.DataFrame:
        """
        Check if the provided EMAs are consistently above or below the current price.

        Args:
            ticker (str): Stock ticker symbol.
            ema_lengths (List[int]): List of EMA window lengths (e.g., [21, 55, 144]).

        Returns:
            pd.DataFrame: DataFrame containing the EMA values and indicators ("TRUE"/"FALSE") for above/below current price.
        """
        try:
            # Build EMA URLs using self.api_key
            urls = [
                f"https://api.polygon.io/v1/indicators/ema/{ticker}?timespan=day&adjusted=true&window={length}&series_type=close&order=desc&apiKey={self.api_key}"
                for length in ema_lengths
            ]

            # Fetch EMA data concurrently
            tasks = [self.fetch(url) for url in urls]
            data_list = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results and extract EMA values
            results = []
            for idx, data in enumerate(data_list):
                if isinstance(data, Exception):
                    logger.error("Error fetching EMA data for length %s: %s", ema_lengths[idx], data, exc_info=True)
                    results.append({})
                else:
                    results.append(data.get("results", {}))

            # Convert EMA data to DataFrames per EMA length
            values_per_ema = []
            for length, result in zip(ema_lengths, results):
                values = result.get("values", [])
                if values:
                    df_ema = pd.DataFrame({
                        "EMA Length": [length] * len(values),
                        "Date": [datetime.fromtimestamp(v["timestamp"] / 1000).strftime('%Y-%m-%d') for v in values],
                        "Value": [v["value"] for v in values],
                    })
                    values_per_ema.append(df_ema)
                else:
                    logger.warning("No EMA values returned for length %s", length)

            if not values_per_ema:
                raise ValueError("No EMA data available for any of the specified lengths.")

            df = pd.concat(values_per_ema, ignore_index=True)
            df["ticker"] = ticker

            # Get the latest EMA values for each EMA length
            latest_ema_values = df.sort_values("Date").groupby("EMA Length").tail(1).reset_index(drop=True)

            # Fetch the current price (assuming get_price is implemented)
            price = await self.get_price(ticker)
            if price is None:
                raise ValueError(f"Unable to fetch current price for ticker {ticker}")

            # Determine if all EMA values are above or below the current price
            above_current_price = all(latest_ema_values["Value"] > price)
            below_current_price = all(latest_ema_values["Value"] < price)
            all_above = "TRUE" if above_current_price else "FALSE"
            all_below = "TRUE" if below_current_price else "FALSE"

            df["above"] = all_above
            df["below"] = all_below

            return df
        except Exception as e:
            logger.error("Error in ema_check for ticker %s: %s", ticker, e, exc_info=True)
            raise

    async def market_news(self, limit: str = "100") -> TickerNews:
        """
        Fetch market news from Polygon.io.

        Args:
            limit (str): The number of news items to return (max 1000).

        Returns:
            TickerNews: Parsed market news data.
        """
        try:
            endpoint = f"https://api.polygon.io/v2/reference/news?limit={limit}&apiKey={self.api_key}"
            data = await self.fetch_endpoint(endpoint)
            results = data.get("results", [])
            return TickerNews(results)
        except Exception as e:
            logger.error("Error fetching market news: %s", e, exc_info=True)
            raise

    async def fetch_endpoint(self, url: str, session: aiohttp.ClientSession = None) -> dict:
        try:
            if session is None:
                async with aiohttp.ClientSession() as new_session:
                    async with new_session.get(url) as response:
                        response.raise_for_status()
                        return await response.json()
            else:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json()
        except Exception as e:
            logger.error("Error fetching endpoint: %s", e, exc_info=True)
            raise
    async def ticker_news(self, ticker:str, limit: str = "100") -> TickerNews:
        """
        Fetch market news from Polygon.io.

        Args:
            limit (str): The number of news items to return (max 1000).

        Returns:
            TickerNews: Parsed market news data.
        """
        try:
            endpoint = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&limit={limit}&apiKey={self.api_key}"
            data = await self.fetch_endpoint(endpoint)
            results = data.get("results", [])
            return TickerNews(results)
        except Exception as e:
            logger.error("Error fetching market news: %s", e, exc_info=True)
            raise


    async def dark_pools(
        self,
        ticker: str,
        multiplier: int,
        timespan: str,
        date_from: str,
        date_to: str
    ) -> pd.DataFrame:
        """
        Analyze dark pool data by filtering aggregate data for high dollar cost trades.

        Args:
            ticker (str): Stock ticker symbol.
            multiplier (int): Aggregation multiplier.
            timespan (str): Aggregation timespan.
            date_from (str): Start date (YYYY-MM-DD).
            date_to (str): End date (YYYY-MM-DD).

        Returns:
            pd.DataFrame: DataFrame containing dark pool trade data where dollar cost exceeds 10M.
        """
        try:
            aggs = await self.get_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                date_from=date_from,
                date_to=date_to
            )
            if not aggs:
                raise ValueError("No aggregate data returned for dark pools analysis.")

            # Assuming 'aggs' has attributes: close, timestamp, and dollar_cost as lists
            details = [
                {
                    "Close Price": aggs.close[i],
                    "Timestamp": aggs.timestamp[i],
                    "Dollar Cost": cost
                }
                for i, cost in enumerate(aggs.dollar_cost)
                if cost > 10000000
            ]

            df_dollar_cost = pd.DataFrame(details)
            df_formatted = format_large_numbers_in_dataframe(df_dollar_cost)
            return df_formatted
        except Exception as e:
            logger.error("Error fetching dark pools data for ticker %s: %s", ticker, e, exc_info=True)
            raise

    async def top_gainers_losers(self, type: str = "gainers") -> GainersLosers:
        """
        Fetch the top gainers or losers of the day.

        Args:
            type (str): Either 'gainers' or 'losers'. Defaults to 'gainers'.

        Returns:
            GainersLosers: Parsed data for the top gainers or losers.
        """
        try:
            endpoint = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/{type}?apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                data = response.json()
                tickers = data.get("tickers", [])
                return GainersLosers(tickers)
        except Exception as e:
            logger.error("Error fetching top %s: %s", type, e, exc_info=True)
            raise

    async def company_info(self, ticker: str) -> Optional[CompanyResults]:
        """
        Get company information for a given ticker.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            Optional[CompanyResults]: Company details if available; otherwise, None.
        """
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                results = data.get("results")
                if results is None:
                    logger.warning("No company info found for ticker: %s", ticker)
                    return None
                return CompanyResults(results)
        except Exception as e:
            logger.error("Error fetching company info for %s: %s", ticker, e, exc_info=True)
            return None

    async def get_all_tickers(self, include_otc: bool = False, save_all_tickers: bool = False) -> StockSnapshot:
        """
        Fetch a list of all stock tickers available on Polygon.io.

        Args:
            include_otc (bool): Whether to include OTC securities. Defaults to False.
            save_all_tickers (bool): Optionally save tickers for later processing. Defaults to False.

        Returns:
            StockSnapshot: An object containing the list of tickers.
        """
        try:
            endpoint = (
                f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?"
                f"apiKey={self.api_key}&include_otc={include_otc}"
            )
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                data = response.json()
                tickers = data.get("tickers", [])
                return StockSnapshot(tickers)
        except Exception as e:
            logger.error("Error fetching all tickers: %s", e, exc_info=True)
            raise

    async def stock_snapshot(self, ticker: str) -> Optional[SingleStockSnapshot]:
        """
        Retrieve a snapshot for a given stock ticker.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            Optional[SingleStockSnapshot]: Snapshot data for the ticker, or None if unavailable.
        """
        try:
            url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                ticker_data = data.get("ticker")
                if ticker_data is None:
                    logger.warning("No snapshot data found for ticker: %s", ticker)
                    return None
                return SingleStockSnapshot(ticker_data)
        except Exception as e:
            logger.error("Error fetching stock snapshot for %s: %s", ticker, e, exc_info=True)
            return None

    async def get_cik(self, ticker: str) -> Optional[str]:
        """
        Get a CIK number for a given ticker.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            Optional[str]: The CIK number if available, else None.
        """
        try:
            endpoint = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                data = response.json()
                cik = data.get("results", {}).get("cik")
                if cik is None:
                    logger.warning("CIK not found for ticker: %s", ticker)
                return cik
        except Exception as e:
            logger.error("Error fetching CIK for %s: %s", ticker, e, exc_info=True)
            return None

    async def macd(self, ticker: str, timespan: str, limit: str = "1000") -> Optional[MACD]:
        """
        Fetch MACD indicator data for the specified ticker.

        Args:
            ticker (str): Stock ticker symbol.
            timespan (str): Data aggregation timespan.
            limit (str): Number of data points to retrieve (default "1000").

        Returns:
            Optional[MACD]: A MACD object if data is returned; otherwise, None.
        """
        try:
            endpoint = (
                f"https://api.polygon.io/v1/indicators/macd/{ticker}"
                f"?timespan={timespan}&adjusted=true&short_window=12&long_window=26&signal_window=9"
                f"&series_type=close&order=desc&apiKey={self.api_key}&limit={limit}"
            )
            async with httpx.AsyncClient() as client:
                response = await client.get(endpoint)
                response.raise_for_status()
                data = response.json()
                if data:
                    return MACD(data, ticker)
                else:
                    logger.warning("No MACD data returned for ticker %s", ticker)
                    return None
        except Exception as e:
            logger.error("Unexpected error in MACD for %s: %s", ticker, e, exc_info=True)
            return None

    async def sma(
        self,
        ticker: str,
        timespan: str,
        limit: str = "1000",
        window: str = "9",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Optional[SMA]:
        """
        Fetch SMA indicator data for a given ticker.

        Args:
            ticker (str): Stock ticker symbol.
            timespan (str): Data aggregation timespan.
            limit (str): Number of data points to retrieve (default "1000").
            window (str): SMA window size (default "9").
            date_from (Optional[str]): Start date (YYYY-MM-DD); defaults to self.eight_days_ago.
            date_to (Optional[str]): End date (YYYY-MM-DD); defaults to self.today.

        Returns:
            Optional[SMA]: An SMA object if data is returned; otherwise, None.
        """
        try:
            if date_from is None:
                date_from = self.eight_days_ago
            if date_to is None:
                date_to = self.today
            endpoint = (
                f"https://api.polygon.io/v1/indicators/sma/{ticker}"
                f"?timespan={timespan}&window={window}&timestamp.gte={date_from}&timestamp.lte={date_to}"
                f"&limit={limit}&apiKey={self.api_key}"
            )
            await self.create_session()
            async with self.session.get(endpoint) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return SMA(data, ticker)
        except Exception as e:
            logger.error("Error fetching SMA for %s: %s", ticker, e, exc_info=True)
            return None

    async def ema(
        self,
        ticker: str,
        timespan: str,
        limit: str = "1",
        window: str = "21",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> Optional[EMA]:
        """
        Fetch EMA indicator data for a given ticker.

        Args:
            ticker (str): Stock ticker symbol.
            timespan (str): Data aggregation timespan.
            limit (str): Number of data points to retrieve (default "1").
            window (str): EMA window size (default "21").
            date_from (Optional[str]): Start date (YYYY-MM-DD); defaults to self.eight_days_ago.
            date_to (Optional[str]): End date (YYYY-MM-DD); defaults to self.today.

        Returns:
            Optional[EMA]: An EMA object if data is returned; otherwise, None.
        """
        try:
            if date_from is None:
                date_from = self.eight_days_ago
            if date_to is None:
                date_to = self.today
            endpoint = (
                f"https://api.polygon.io/v1/indicators/ema/{ticker}"
                f"?timespan={timespan}&window={window}&timestamp.gte={date_from}&timestamp.lte={date_to}"
                f"&limit={limit}&apiKey={self.api_key}"
            )
            await self.create_session()
            async with self.session.get(endpoint) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return EMA(data, ticker)
        except Exception as e:
            logger.error("Error fetching EMA for %s: %s", ticker, e, exc_info=True)
            return None

    async def get_price(self, ticker: str) -> Optional[float]:
        """
        Get the price of a ticker, index, option, or crypto coin.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            Optional[float]: The latest price if available; otherwise, None.
        """
        try:
            if ticker in ["SPX", "NDX", "XSP", "RUT", "VIX"]:
                ticker = f"I:{ticker}"
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                results = data.get("results")
                if results:
                    sessions = [item.get("session") for item in results]
                    prices = [s.get("close") for s in sessions if s is not None]
                    if prices:
                        return prices[0]
                    else:
                        logger.warning("No price found in session data for ticker %s", ticker)
                        return None
                else:
                    logger.warning("No results found for ticker %s", ticker)
                    return None
        except Exception as e:
            logger.error("Error fetching price for %s: %s", ticker, e, exc_info=True)
            return None

    async def rsi_snapshot(self, tickers: Union[str, List[str]]) -> Optional[pd.DataFrame]:
        """
        Gather a snapshot of the RSI across multiple timespans.
        If a single ticker is provided (as a string), the snapshot is generated for that ticker.
        If a list of tickers is provided, data is aggregated for each ticker.

        Args:
            tickers (Union[str, List[str]]): A single ticker symbol or a list of ticker symbols.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing RSI data; or None if no data is available.
        """
        if isinstance(tickers, str):
            # Single-ticker snapshot using internal RSI tasks (assumes self.rsi exists)
            try:
                timespans = ["minute", "day", "hour", "week", "month"]
                rsi_tasks = {ts: asyncio.create_task(self.rsi(tickers, timespan=ts)) for ts in timespans}
                results = await asyncio.gather(*rsi_tasks.values(), return_exceptions=True)
                price = results[0]
                rsi_results = dict(zip(timespans, results[1:]))
                if price is None or isinstance(price, Exception):
                    logger.error("Failed to fetch price for ticker %s: %s", tickers, price)
                    return None
                rsis = {"ticker": tickers, "price": price}
                for ts, res in rsi_results.items():
                    if isinstance(res, Exception):
                        logger.error("Error fetching RSI for %s at timespan '%s': %s", tickers, ts, res)
                        continue
                    rsi_value = self.extract_rsi_value(res)
                    if rsi_value is not None:
                        rsis[f"{ts}_rsi"] = rsi_value
                    else:
                        logger.warning("No RSI data for ticker %s at timespan '%s'", tickers, ts)
                if len(rsis) > 2:
                    return pd.DataFrame([rsis])
                else:
                    logger.warning("No RSI data available for ticker %s", tickers)
                    return None
            except Exception as e:
                logger.error("Exception in rsi_snapshot for ticker %s: %s", tickers, e, exc_info=True)
                return None
        elif isinstance(tickers, list):
            # Multi-ticker snapshot using concurrent fetches
            try:
                timespans = ["minute", "day", "hour", "week", "month"]
                tasks = [self.fetch_rsi_for_tickers(tickers, timespan=ts) for ts in timespans]
                rsi_results = await asyncio.gather(*tasks, return_exceptions=True)
                aggregated_data: Dict[str, Dict[str, float]] = {}
                for ts, result in zip(timespans, rsi_results):
                    if isinstance(result, Exception):
                        logger.error("Error fetching RSI data for timespan '%s': %s", ts, result, exc_info=True)
                        continue
                    for tkr, data in result.items():
                        if tkr not in aggregated_data:
                            aggregated_data[tkr] = {}
                        rsi_value = self.extract_rsi_value(data)
                        if rsi_value is not None:
                            aggregated_data[tkr][f"{ts}_rsi"] = rsi_value
                records = []
                for tkr, values in aggregated_data.items():
                    record = {"ticker": tkr}
                    record.update(values)
                    if values:
                        records.append(record)
                if records:
                    return pd.DataFrame(records)
                else:
                    logger.warning("No RSI data available for provided tickers.")
                    return None
            except Exception as e:
                logger.error("Exception in rsi_snapshot for tickers %s: %s", tickers, e, exc_info=True)
                return None
        else:
            logger.error("Invalid input type for tickers: %s", type(tickers))
            return None

    def extract_rsi_value(self, rsi_data: Any) -> Optional[float]:
        """
        Helper method to extract the most recent RSI value safely.

        Args:
            rsi_data (Any): RSI data structure expected to contain 'results' and 'values'.

        Returns:
            Optional[float]: The latest RSI value if available; otherwise, None.
        """
        try:
            if rsi_data and "results" in rsi_data:
                values = rsi_data["results"].get("values")
                if values and len(values) > 0:
                    return values[-1]["value"]
        except Exception as e:
            logger.error("Error extracting RSI value: %s", e, exc_info=True)
        return None

    async def check_macd_sentiment(self, hist: List[float]) -> Optional[str]:
        """
        Check MACD histogram values for bullish or bearish sentiment.

        Args:
            hist (List[float]): List of MACD histogram values.

        Returns:
            Optional[str]: 'bullish' if conditions are met, 'bearish' if conditions are met, '-' otherwise.
        """
        try:
            if hist and len(hist) >= 3:
                last_three = hist[:3]
                if abs(last_three[0] - (-0.02)) < 0.04 and all(
                    last_three[i] > last_three[i + 1] for i in range(len(last_three) - 1)
                ):
                    return "bullish"
                if abs(last_three[0] - 0.02) < 0.04 and all(
                    last_three[i] < last_three[i + 1] for i in range(len(last_three) - 1)
                ):
                    return "bearish"
            return "-"
        except Exception as e:
            logger.error("Error checking MACD sentiment: %s", e, exc_info=True)
            return None

    async def histogram_snapshot(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Returns a DataFrame of bullish/bearish imminent MACD cross sentiment if detected.

        Args:
            ticker (str): Stock ticker symbol.

        Returns:
            Optional[pd.DataFrame]: DataFrame with MACD sentiment per timespan and ticker column.
        """
        try:
            timespans = ["day", "hour", "week", "month"]
            macd_tasks = {
                ts: asyncio.create_task(self.macd(ticker, timespan=ts, limit="10"))
                for ts in timespans
            }
            results = await asyncio.gather(*macd_tasks.values(), return_exceptions=True)
            macd_results = dict(zip(timespans, results))

            histograms = {}
            sentiment_tasks = {}

            for ts, macd_result in macd_results.items():
                if isinstance(macd_result, Exception):
                    logger.error("Error fetching MACD for %s at timespan '%s': %s", ticker, ts, macd_result, exc_info=True)
                    continue
                if macd_result and hasattr(macd_result, "macd_histogram") and macd_result.macd_histogram and len(macd_result.macd_histogram) > 2:
                    hist = macd_result.macd_histogram
                    sentiment_tasks[ts] = asyncio.create_task(self.check_macd_sentiment(hist))
                else:
                    logger.warning("Invalid MACD histogram data for %s at timespan '%s'.", ticker, ts)

            if sentiment_tasks:
                sentiment_results = await asyncio.gather(*sentiment_tasks.values(), return_exceptions=True)
                for ts, sentiment in zip(sentiment_tasks.keys(), sentiment_results):
                    if isinstance(sentiment, Exception):
                        logger.error("Error in check_macd_sentiment for %s at timespan '%s': %s", ticker, ts, sentiment, exc_info=True)
                    else:
                        histograms[f"{ts}_macd"] = sentiment

            df = pd.DataFrame([histograms])
            df["ticker"] = ticker
            return df
        except Exception as e:
            logger.error("Exception in histogram_snapshot for ticker %s: %s", ticker, e, exc_info=True)
            return None

    async def get_polygon_logo(self, symbol: str) -> Optional[str]:
        """
        Fetch the URL of the logo for the given stock symbol from Polygon.io.

        Args:
            symbol (str): Stock symbol.

        Returns:
            Optional[str]: URL of the logo if available; otherwise, None.
        """
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={self.api_key}"
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                if "results" not in data:
                    return None
                results = data["results"]
                branding = results.get("branding")
                if branding and "icon_url" in branding:
                    encoded_url = branding["icon_url"]
                    decoded_url = unquote(encoded_url)
                    return f"{decoded_url}?apiKey={self.api_key}"
                return None
        except Exception as e:
            logger.error("Error fetching polygon logo for symbol %s: %s", symbol, e, exc_info=True)
            return None

    async def stock_trades(
        self,
        ticker: str,
        limit: str = "50000",
        timestamp_gte: Optional[str] = None,
        timestamp_lte: Optional[str] = None,
    ) -> TradeData:
        """
        Get trades for a ticker. Defaults to 50,000 results and a timestamp range.

        Args:
            ticker (str): Stock ticker symbol.
            limit (str): Number of trades to return.
            timestamp_gte (Optional[str]): Start timestamp (YYYY-MM-DD); defaults to self.thirty_days_ago.
            timestamp_lte (Optional[str]): End timestamp (YYYY-MM-DD); defaults to self.today.

        Returns:
            TradeData: An object containing the trade data.
        """
        try:
            if timestamp_gte is None:
                timestamp_gte = self.thirty_days_ago
            if timestamp_lte is None:
                timestamp_lte = self.today

            endpoint = (
                f"https://api.polygon.io/v3/trades/{ticker}"
                f"?timestamp.gte={timestamp_gte}"
                f"&timestamp.lte={timestamp_lte}"
                f"&apiKey={self.api_key}"
                f"&limit={limit}"
            )
            data = await self.paginate_concurrent(endpoint, as_dataframe=False)
            return TradeData(data, ticker=ticker)
        except Exception as e:
            logger.error("Error fetching stock trades for %s: %s", ticker, e, exc_info=True)
            raise

    async def get_prices(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        """
        Get the prices of multiple tickers in a single API call.

        Args:
            tickers (List[str]): List of stock ticker symbols.

        Returns:
            Dict[str, Optional[float]]: Mapping from ticker symbol to its latest price.
        """
        try:
            tickers = [
                f"I:{t}" if t in ["SPX", "NDX", "XSP", "RUT", "VIX"] else t for t in tickers
            ]
            tickers_str = ",".join(tickers)
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={tickers_str}&apiKey={self.api_key}"
            await self.create_session()
            response = await self.session.get(url)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            prices = {}
            for item in results:
                ticker_symbol = item.get("ticker")
                session_data = item.get("session", {})
                close_price = session_data.get("close")
                prices[ticker_symbol] = close_price
            return prices
        except Exception as e:
            logger.error("Error fetching prices: %s", e, exc_info=True)
            return {}

    async def fetch_latest_rsi(
        self, session: aiohttp.ClientSession, ticker: str, timespan: str = "day"
    ) -> Tuple[str, Optional[float]]:
        """
        Fetch the latest RSI value for a single ticker.

        Args:
            session (aiohttp.ClientSession): An active aiohttp session.
            ticker (str): Stock ticker symbol.
            timespan (str): Timespan for RSI (default "day").

        Returns:
            Tuple[str, Optional[float]]: A tuple of the ticker and its latest RSI value (or None).
        """
        params = {
            "timespan": timespan,
            "adjusted": "true",
            "window": "14",
            "series_type": "close",
            "order": "desc",
            "limit": "1",
            "apiKey": self.api_key,
        }
        url = f"https://api.polygon.io/v1/indicators/rsi/{ticker}"
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                results = data.get("results", {})
                values = results.get("values", [])
                if values:
                    return ticker, values[0]["value"]
                else:
                    return ticker, None
        except Exception as e:
            logger.error("Error fetching latest RSI for %s: %s", ticker, e, exc_info=True)
            return ticker, None

    async def fetch_rsi_for_tickers(self, tickers: List[str], timespan: str = "day") -> Dict[str, Optional[float]]:
        """
        Fetch the latest RSI for multiple tickers concurrently.

        Args:
            tickers (List[str]): List of stock ticker symbols.
            timespan (str): Timespan for RSI (default "day").

        Returns:
            Dict[str, Optional[float]]: Mapping from ticker to its latest RSI value.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(self.fetch_latest_rsi(session, ticker, timespan=timespan))
                for ticker in tickers
            ]
            results = await asyncio.gather(*tasks)
            return dict(results)

    async def fetch_rsi_data(self, endpoint: str, ticker: str) -> Tuple[str, Any]:
        """
        Fetch RSI data from a given endpoint for a ticker.

        Args:
            endpoint (str): The API endpoint.
            ticker (str): Stock ticker symbol.

        Returns:
            Tuple[str, Any]: A tuple of the ticker and the fetched RSI data.
        """
        try:
            await self.create_session()
            async with self.session.get(endpoint) as response:
                response.raise_for_status()
                data = await response.json()
                return ticker, data
        except Exception as e:
            logger.error("Error fetching RSI data for %s: %s", ticker, e, exc_info=True)
            return ticker, None

    async def stock_snapshots(self) -> Optional[StockSnapshot]:
        """
        Fetch snapshots for multiple tickers and return a StockSnapshot object.

        Returns:
            Optional[StockSnapshot]: A StockSnapshot instance if data is retrieved; otherwise, None.
        """
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={self.api_key}"
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    snapshots = data.get("tickers", [])
                    return StockSnapshot(snapshots)
        except Exception as e:
            logger.error("Error fetching stock snapshots: %s", e, exc_info=True)
            return None

    async def fetch_macd(
        self, session: aiohttp.ClientSession, ticker: str, timespan: str = "day"
    ) -> Dict[str, Any]:
        """
        Fetch the last 3 MACD data points for the given ticker.
        
        Args:
            session (aiohttp.ClientSession): An active aiohttp session.
            ticker (str): Stock ticker symbol.
            timespan (str): Timespan for the data (default is 'day').
        
        Returns:
            Dict[str, Any]: A dictionary in the format:
                {
                    "ticker": <ticker>,
                    "hist_values": [hist1, hist2, hist3, ...]  # Newest first
                }
        """
        params = {
            "timespan": timespan,
            "adjusted": "true",
            "short_window": "12",
            "long_window": "26",
            "signal_window": "9",
            "series_type": "close",
            "order": "desc",   # Newest data first
            "limit": "3",      # Retrieve 3 data points
            "apiKey": self.api_key,
        }
        url = f"https://api.polygon.io/v1/indicators/macd/{ticker}"
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                results = data.get("results", {})
                values = results.get("values", [])
                hist_values = []
                for item in values:
                    h = item.get("histogram")
                    if h is not None:
                        hist_values.append(h)
                return {"ticker": ticker, "hist_values": hist_values}
        except Exception as e:
            logger.error("Error fetching MACD for %s: %s", ticker, e, exc_info=True)
            return {"ticker": ticker, "hist_values": []}

    def check_macd_sentiment(self, hist: List[float]) -> str:
        """
        Analyze the MACD histogram to determine if the sentiment is bullish or bearish.
        
        Args:
            hist (List[float]): Histogram values in reverse-chronological order (newest first).
        
        Returns:
            str: 'bullish' if a bullish setup is detected, 'bearish' if bearish, or '-' if no clear signal.
        """
        try:
            if not hist or len(hist) < 3:
                return '-'
            last_three = hist[:3]
            # Check for bullish sentiment: first value close to -0.02 and trending down
            if (
                abs(last_three[0] + 0.02) < 0.04
                and all(last_three[i] > last_three[i + 1] for i in range(len(last_three) - 1))
            ):
                return "bullish"
            # Check for bearish sentiment: first value close to +0.02 and trending up
            if (
                abs(last_three[0] - 0.02) < 0.04
                and all(last_three[i] < last_three[i + 1] for i in range(len(last_three) - 1))
            ):
                return "bearish"
            return "-"
        except Exception as e:
            logger.error("Error analyzing MACD sentiment: %s", e, exc_info=True)
            return "-"

    async def fetch_macd_signals_for_tickers(
        self, tickers: List[str], timespan: str = "day"
    ) -> Dict[str, str]:
        """
        Concurrently fetch MACD data (histogram) for each ticker and determine sentiment.
        
        Args:
            tickers (List[str]): List of stock ticker symbols.
            timespan (str): Timespan for data (default is 'day').
        
        Returns:
            Dict[str, str]: A mapping from each ticker to its MACD sentiment ("bullish", "bearish", or "-").
        """
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.create_task(self.fetch_macd(session, t, timespan=timespan)) for t in tickers]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        signals: Dict[str, str] = {}
        for result in results:
            if isinstance(result, Exception):
                # If an error occurred in a task, log and continue.
                logger.error("Error in MACD task: %s", result, exc_info=True)
                continue
            ticker = result.get("ticker", "")
            hist_values = result.get("hist_values", [])
            sentiment = self.check_macd_sentiment(hist_values)
            signals[ticker] = sentiment
        return signals

    async def news_sentiment(self, ticker: str, limit: str = "25") -> Optional[pd.DataFrame]:
        """
        Fetch news for a given ticker and extract sentiment-related information.
        
        Args:
            ticker (str): Stock ticker symbol.
            limit (str): Number of news items to retrieve (default is '25').
        
        Returns:
            Optional[pd.DataFrame]: DataFrame containing news details and associated sentiment; 
            or None if an error occurs.
        """
        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&limit={limit}&apiKey={self.api_key}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    results = data.get("results", [])
                    rows = []
                    for article in results:
                        title = article.get("title")
                        description = article.get("description")
                        pub_time = article.get("published_utc")
                        keywords = article.get("keywords")
                        tickers_list = article.get("tickers")
                        keywords_str = ",".join(keywords) if keywords else ""
                        tickers_str = ",".join(tickers_list) if tickers_list else ""
                        insights = article.get("insights", [])
                        if insights:
                            sentiments = [str(ins.get("sentiment")) for ins in insights if ins.get("sentiment")]
                            sentiment_reason = [str(ins.get("sentiment_reasoning")) for ins in insights if ins.get("sentiment_reasoning")]
                            sentiments_str = ", ".join(sentiments)
                            sentiment_reason_str = ", ".join(sentiment_reason)
                        else:
                            sentiments_str = ""
                            sentiment_reason_str = ""
                        row = {
                            "title": title,
                            "description": description,
                            "sentiment": sentiments_str,
                            "sentiment_reason": sentiment_reason_str,
                            "pub_time": pub_time,
                            "keywords": keywords_str,
                            "tickers": tickers_str,
                        }
                        rows.append(row)
                    df = pd.DataFrame(rows)
                    return df
        except Exception as e:
            logger.error("Error fetching news sentiment for ticker %s: %s", ticker, e, exc_info=True)
            return None