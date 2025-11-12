import pandas as pd
from urllib.parse import urlencode
from fudstop4.apis.polygonio.mapping import option_condition_dict, OPTIONS_EXCHANGES
from datetime import datetime, timedelta, timezone
import pytz
import aiohttp
import numpy as np
import requests
from fudstop4.apis.helpers import get_human_readable_string
from typing import Optional
from fudstop4.apis.webull.trade_models.stock_quote import MultiQuote
import asyncio
import httpx
from typing import List, Dict
import time
import logging
from fudstop4.apis.webull.webull_trading import WebullTrading
from fudstop4.apis.polygonio.async_polygon_sdk import Polygon
from fudstop4.apis.polygonio.models.option_models.universal_snapshot import UniversalOptionSnapshot
from fudstop4.apis.webull.trade_models.analyst_ratings import Analysis
from fudstop4.apis.webull.trade_models.stock_quote import MultiQuote
from fudstop4.apis.webull.trade_models.capital_flow import CapitalFlow, CapitalFlowHistory
from fudstop4.apis.webull.trade_models.deals import Deals
from fudstop4.apis.webull.trade_models.cost_distribution import CostDistribution, NewCostDist
from fudstop4.apis.webull.trade_models.etf_holdings import ETFHoldings
from fudstop4.apis.webull.webull_ta import WebullTA
from fudstop4.apis.webull.trade_models.institutional_holdings import InstitutionHolding, InstitutionStat
from fudstop4.apis.webull.trade_models.financials import BalanceSheet, FinancialStatement, CashFlow
from fudstop4.apis.webull.trade_models.news import NewsItem
from fudstop4.apis.webull.trade_models.forecast_evaluator import ForecastEvaluator
from fudstop4.apis.webull.trade_models.short_interest import ShortInterest
from fudstop4.apis.webull.webull_option_screener import WebullOptionScreener
from fudstop4.apis.helpers import generate_webull_headers
from fudstop4.apis.webull.trade_models.volume_analysis import WebullVolAnalysis
from fudstop4.apis.webull.trade_models.ticker_query import WebullStockData
from fudstop4.apis.webull.trade_models.analyst_ratings import Analysis
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.webull.trade_models.price_streamer import PriceStreamer
from fudstop4.apis.webull.trade_models.company_brief import CompanyBrief, Executives, Sectors
from fudstop4.apis.webull.trade_models.order_flow import OrderFlow
import asyncio
trading= WebullTrading()
import aiohttp
import os
from dotenv import load_dotenv
poly = Polygon()
load_dotenv()
ta = WebullTA()
db = PolygonOptions(database='fudstop3')
import redis.asyncio as redis
class RedisCacheManager:
    """
    Manages async Redis connections and provides simple
    methods to get/set candle data with a TTL.
    """
    def __init__(self, redis_url: str = "redis://localhost:6379", default_ttl: int = 300):
        """
        :param redis_url: Redis connection string.
        :param default_ttl: Default TTL in seconds for cached items.
        """
        self.redis_url = redis_url
        self.default_ttl = default_ttl
        self._redis = None

    async def connect(self):
        """
        Establish a connection to Redis (call once).
        Using redis.asyncio, from_url() returns an
        asyncio-compatible Redis client.
        """
        self._redis = redis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True
        )

    async def close(self):
        """
        Close the Redis connection gracefully.
        In redis.asyncio, we do:
           self._redis.close()
           await self._redis.wait_closed()
        """
        if self._redis:
            self._redis.close()
            await self._redis.wait_closed()

    async def get_cached_candles(self, key: str) -> Optional[pd.DataFrame]:
        """
        Retrieve candle data from Redis by key.
        We store the DataFrame in a serialized form (e.g., JSON).
        """
        if not self._redis:
            raise RuntimeError("Redis connection not established. Call connect() first.")

        cached_data = await self._redis.get(key)
        if cached_data:
            try:
                # Reconstruct a DataFrame
                df = pd.read_json(cached_data, orient='split')
                return df
            except Exception as e:
                print(f"Failed to load cached DataFrame for key={key}: {e}")
                return None
        return None

    async def set_cached_candles(self, key: str, df: pd.DataFrame, ttl: Optional[int] = None):
        """
        Store candle DataFrame in Redis for the given key.
        By default uses self.default_ttl for expiration.
        """
        if not self._redis:
            raise RuntimeError("Redis connection not established. Call connect() first.")

        if ttl is None:
            ttl = self.default_ttl

        try:
            # Convert DataFrame to JSON (orient='split' preserves columns, index, data)
            serialized = df.to_json(orient='split')
            await self._redis.set(key, serialized, ex=ttl)
        except Exception as e:
            print(f"Failed to set cached DataFrame for key={key}: {e}")













class UltimateSDK:
    def __init__(self, redis_cache:RedisCacheManager=None):
        self.redis_cache = redis_cache
    # ---------------------------------------------------------------
    # SINGLE-TICKER METHODS (as you already have them)
    # ---------------------------------------------------------------
      

        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.scalar_tickers = ['SPX', 'VIX', 'OSTK', 'XSP', 'NDX', 'MXEF']
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.semaphore = asyncio.Semaphore(40)
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.timeframes = ['m1','m5', 'm10', 'm15', 'm20', 'm30', 'm60', 'm120', 'm240', 'd1']
        self.now_timestamp_int = int(datetime.now(timezone.utc).timestamp())
        self.day = int(86400)
        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.id = 15765933
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))

        

    async def get_quotes_for_tickers(self, tickers: List[str]):
        async def chunk_and_get_ids(lst, chunk_size):
            """Asynchronously chunk the list and fetch IDs for each chunk."""
            for i in range(0, len(lst), chunk_size):
                chunk = lst[i:i + chunk_size]
                ids = await self.get_webull_id_for_tickers(chunk)  # Fetch IDs for the current chunk
                yield ids
        
        results = []
        
        async with httpx.AsyncClient() as client:
            # Asynchronously process chunks and make API requests
            async for ticker_ids in chunk_and_get_ids(tickers, 54):
                ticker_ids_str = ",".join(map(str, ticker_ids)) # Join IDs into a comma-separated string
                response = await client.get(
                    f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_ids_str}&includeSecu=1&delay=0&more=1"
                )
                if response.status_code == 200:
                    # Assuming the API returns JSON data
                    results.extend(response.json())
                else:
                    # Handle errors (optional logging)
                    print(f"Failed to fetch data for IDs: {ticker_ids}")

        return MultiQuote(results)
    


    async def get_webull_id_for_tickers(self, tickers: List[str]):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_ids = [self.ticker_to_id_map.get(symbol) for symbol in tickers]
        # Remove None values from the list
        filtered_ticker_ids = [ticker_id for ticker_id in ticker_ids if ticker_id is not None]
        return filtered_ticker_ids

    async def get_webull_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_id = self.ticker_to_id_map.get(symbol)
        return ticker_id

    async def get_analyst_ratings(self, symbol: str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/information/securities/analysis?tickerId={ticker_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint) as resp:
                    if resp.status == 200:
                        datas = await resp.json()
                        return Analysis(datas)
        except Exception as e:
            print(e)
        return None

    async def get_short_interest(self, symbol: str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")

            endpoint = f"https://quotes-gw.webullfintech.com/api/information/brief/shortInterest?tickerId={ticker_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint) as resp:
                    if resp.status == 200:
                        datas = await resp.json()
                        return ShortInterest(datas)
        except Exception as e:
            print(f"Error: {e}")
        return None

    async def institutional_holding(self, symbol: str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/information/stock/getInstitutionalHolding?tickerId={ticker_id}"
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint) as resp:
                    if resp.status == 200:
                        datas = await resp.json()
                        return InstitutionStat(datas)
        except Exception as e:
            print(e)
        return None

    async def volume_analysis(self, symbol: str):
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count=10&tickerId={ticker_id}&type=0"
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint) as resp:
                    if resp.status == 200:
                        datas = await resp.json()
                        return WebullVolAnalysis(datas, symbol)
        except Exception as e:
            print(e)
        return None

    async def new_cost_dist(self, symbol: str, start_date: str, end_date: str):
        """Returns list"""
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            endpoint = (
                f"https://quotes-gw.webullfintech.com/api/quotes/chip/query?"
                f"tickerId={ticker_id}&startDate={start_date}&endDate={end_date}"
            )
            async with aiohttp.ClientSession() as session:
                async with session.get(endpoint) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        data = data['data']
                        return NewCostDist(data, symbol)
        except Exception as e:
            print(e)
        return None

    # ---------------------------------------------------------------
    # MULTI-TICKER METHODS (concurrent versions)
    # ---------------------------------------------------------------

    async def institutional_holdings_for_tickers(
        self, 
        tickers: list[str]
    ) -> dict[str, InstitutionStat | None]:
        """
        Fetch institutional holding for multiple tickers concurrently.
        Returns a dict: { ticker: InstitutionStat object (or None) }
        """
        tasks = [
            asyncio.create_task(self.institutional_holding(ticker))
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def short_interest_for_tickers(
        self, 
        tickers: list[str]
    ) -> dict[str, ShortInterest | None]:
        """
        Fetch short interest for multiple tickers concurrently.
        Returns a dict: { ticker: ShortInterest object (or None) }
        """
        tasks = [
            asyncio.create_task(self.get_short_interest(ticker))
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def analyst_ratings_for_tickers(
        self, 
        tickers: list[str]
    ) -> dict[str, Analysis | None]:
        """
        Fetch analyst ratings for multiple tickers concurrently.
        Returns a dict: { ticker: Analysis object (or None) }
        """
        tasks = [
            asyncio.create_task(self.get_analyst_ratings(ticker))
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def volume_analysis_for_tickers(
        self, 
        tickers: list[str]
    ) -> dict[str, WebullVolAnalysis | None]:
        """
        Fetch volume analysis for multiple tickers concurrently.
        Returns a dict: { ticker: WebullVolAnalysis object (or None) }
        """
        tasks = [
            asyncio.create_task(self.volume_analysis(ticker))
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def new_cost_dist_for_tickers(
        self, 
        tickers: list[str], 
        start_date: str, 
        end_date: str
    ) -> dict[str, NewCostDist | None]:
        """
        Fetch new cost dist for multiple tickers concurrently (requires start_date & end_date).
        Returns a dict: { ticker: NewCostDist object (or None) }
        """
        tasks = [
            asyncio.create_task(self.new_cost_dist(ticker, start_date, end_date))
            for ticker in tickers
        ]
        results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def _news_single(self, session: httpx.AsyncClient, symbol: str, pageSize: str, headers) -> "NewsItem | None":
        """
        Private helper for fetching news for a single ticker using an existing session.
        """
        try:
            if not headers:
                raise ValueError("Headers are required but not provided.")

            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found in ticker_to_id_map.")

            endpoint = (
                "https://nacomm.webullfintech.com/api/information/news/"
                f"tickerNews?tickerId={ticker_id}&currentNewsId=0&pageSize={pageSize}"
            )
            # session is already created by the caller
            response = await session.get(endpoint)
            if response.status_code == 200:
                datas = response.json()
                return NewsItem(datas)  # your existing data class
            else:
                raise Exception(f"Failed to fetch news data: {response.status_code}")
        except Exception as e:
            print(f"Error in news: {symbol}, {e}")
            return None

    async def _company_brief_single(self, session: httpx.AsyncClient, symbol: str) -> tuple | None:
        """
        Private helper to fetch a company's brief for a single ticker.
        Returns (CompanyBrief, Sectors, Executives) or None.
        """
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = f"https://quotes-gw.webullfintech.com/api/information/stock/brief?tickerId={ticker_id}"
            resp = await session.get(endpoint)
            if resp.status_code != 200:
                raise Exception(f"HTTP error: {resp.status_code}")

            datas = resp.json()
            # Your existing data classes
            companyBrief = CompanyBrief(datas["companyBrief"])
            sectors = Sectors(datas["sectors"])
            executives = Executives(datas["executives"])
            return (companyBrief, sectors, executives)
        except Exception as e:
            print(f"Error in company_brief: {symbol}, {e}")
            return None

    async def _balance_sheet_single(self, session: httpx.AsyncClient, symbol: str, limit: str) -> "BalanceSheet | None":
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = (
                "https://quotes-gw.webullfintech.com/api/information/financial/"
                f"balancesheet?tickerId={ticker_id}&type=101&fiscalPeriod=0&limit={limit}"
            )
            resp = await session.get(endpoint)
            if resp.status_code == 200:
                datas = resp.json()
                return BalanceSheet(datas)
        except Exception as e:
            print(f"Error in balance_sheet: {symbol}, {e}")
        return None
    def balance_sheet_for_tickers_sync(self, tickers: list[str], limit: str = "11") -> dict[str, "BalanceSheet | None"]:
        """
        Fetch balance sheets for multiple tickers synchronously.
        Returns {ticker: BalanceSheet or None}.
        """
        results = {}
        for symbol in tickers:
            result = self._balance_sheet_single_sync(symbol, limit)
            results[symbol] = result
        return results

    def _balance_sheet_single_sync(self, symbol: str, limit: str) -> "BalanceSheet | None":
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")
            
            endpoint = (
                "https://quotes-gw.webullfintech.com/api/information/financial/"
                f"balancesheet?tickerId={ticker_id}&type=101&fiscalPeriod=0&limit={limit}"
            )
            resp = requests.get(endpoint)
            if resp.status_code == 200:
                datas = resp.json()
                return BalanceSheet(datas)
        except Exception as e:
            print(f"Error in balance_sheet: {symbol}, {e}")
        return None
    async def _cash_flow_single(self, session: httpx.AsyncClient, symbol: str, limit: str) -> "CashFlow | None":
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = (
                "https://quotes-gw.webullfintech.com/api/information/financial/"
                f"cashflow?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
            )
            resp = await session.get(endpoint)
            if resp.status_code == 200:
                datas = resp.json()
                return CashFlow(datas)
        except Exception as e:
            print(f"Error in cash_flow: {symbol}, {e}")
        return None

    async def _income_statement_single(self, session: httpx.AsyncClient, symbol: str, limit: str) -> "FinancialStatement | None":
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = (
                "https://quotes-gw.webullfintech.com/api/information/financial/"
                f"incomestatement?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
            )
            resp = await session.get(endpoint)
            if resp.status_code == 200:
                datas = resp.json()
                return FinancialStatement(datas)
        except Exception as e:
            print(f"Error in income_statement: {symbol}, {e}")
        return None

    async def _order_flow_single(self, session: httpx.AsyncClient, symbol: str, headers, flow_type: str, count: str) -> "OrderFlow | None":
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = (
                "https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?"
                f"count={count}&tickerId={ticker_id}&type={flow_type}"
            )
            resp = await session.get(endpoint)
            if resp.status_code == 200:
                data = resp.json()
                return OrderFlow(data)
            else:
                raise Exception(f"Failed to fetch order flow data. HTTP Status: {resp.status_code}")
        except Exception as e:
            print(f"Error in order_flow: {symbol}, {e}")
            return None

    async def _capital_flow_single(self, session: httpx.AsyncClient, symbol: str) -> tuple["CapitalFlow | None", "CapitalFlowHistory | None"]:
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = (
                "https://quotes-gw.webullfintech.com/api/stock/capitalflow/"
                f"ticker?tickerId={ticker_id}&showHis=true"
            )
            resp = await session.get(endpoint)
            resp.raise_for_status()
            datas = resp.json()

            latest = datas.get("latest", {})
            historical = datas.get("historical", [])

            dates = [i.get("date") for i in historical]
            historical_items = [i.get("item") for i in historical]
            latest_item = latest.get("item", {})

            data = CapitalFlow(latest_item, ticker=symbol)
            history = CapitalFlowHistory(historical_items, ticker=symbol)
            return history
        except httpx.RequestError as req_err:
            print(f"Request error for {symbol}: {req_err}")
        except httpx.HTTPStatusError as http_err:
            print(f"HTTP status error for {symbol}: {http_err}")
        except Exception as e:
            print(f"An unexpected error occurred: {symbol}, {e}")

        return None, None

    async def _etf_holdings_single(self, session: httpx.AsyncClient, symbol: str, pageSize: str) -> "ETFHoldings | None":
        try:
            ticker_id = self.ticker_to_id_map.get(symbol)
            if not ticker_id:
                raise ValueError(f"Ticker {symbol} not found.")

            endpoint = (
                "https://quotes-gw.webullfintech.com/api/information/"
                f"company/queryEtfList?tickerId={ticker_id}&pageIndex=1&pageSize={pageSize}"
            )
            resp = await session.get(endpoint)
            if resp.status_code == 200:
                datas = resp.json()
                return ETFHoldings(datas)
        except Exception as e:
            print(f"Error in etf_holdings: {symbol}, {e}")
        return None

    # -------------------------------------------
    # "for_tickers" methods (concurrent)
    # -------------------------------------------

    async def news_for_tickers(
        self, 
        tickers: list[str], 
        pageSize: str = "100", 
        headers=None
    ) -> dict[str, "NewsItem | None"]:
        """
        Fetch news for multiple tickers concurrently using a single session.
        Returns a dict {ticker: NewsItem or None}.
        """
        async with httpx.AsyncClient(headers=headers) as session:
            tasks = [
                asyncio.create_task(self._news_single(session, sym, pageSize, headers))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def company_brief_for_tickers(
        self, 
        tickers: list[str]
    ) -> dict[str, tuple | None]:
        """
        Fetch company briefs for multiple tickers concurrently.
        Returns {ticker: (companyBrief, sectors, executives) or None}.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                asyncio.create_task(self._company_brief_single(session, sym))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def balance_sheet_for_tickers(
        self, 
        tickers: list[str], 
        limit: str = "11"
    ) -> dict[str, "BalanceSheet | None"]:
        """
        Fetch balance sheets for multiple tickers concurrently.
        Returns {ticker: BalanceSheet or None}.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                asyncio.create_task(self._balance_sheet_single(session, sym, limit))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def cash_flow_for_tickers(
        self, 
        tickers: list[str], 
        limit: str = "12"
    ) -> dict[str, "CashFlow | None"]:
        """
        Fetch cash flow statements for multiple tickers concurrently.
        Returns {ticker: CashFlow or None}.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                asyncio.create_task(self._cash_flow_single(session, sym, limit))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def income_statement_for_tickers(
        self, 
        tickers: list[str], 
        limit: str = "12"
    ) -> dict[str, "FinancialStatement | None"]:
        """
        Fetch income statements for multiple tickers concurrently.
        Returns {ticker: FinancialStatement or None}.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                asyncio.create_task(self._income_statement_single(session, sym, limit))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def order_flow_for_tickers(
        self, 
        tickers: list[str], 
        headers, 
        flow_type: str = "0", 
        count: str = "1"
    ) -> dict[str, "OrderFlow | None"]:
        """
        Fetch order flow data for multiple tickers concurrently.
        Returns {ticker: OrderFlow or None}.
        """
        async with httpx.AsyncClient(headers=headers) as session:
            tasks = [
                asyncio.create_task(self._order_flow_single(session, sym, headers, flow_type, count))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def capital_flow_for_tickers(
        self, 
        tickers: list[str]
    ) -> dict[str, tuple["CapitalFlow | None", "CapitalFlowHistory | None"]]:
        """
        Fetch capital flow data (latest + history) for multiple tickers concurrently.
        Returns {ticker: (CapitalFlow, CapitalFlowHistory) or (None, None)}.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                asyncio.create_task(self._capital_flow_single(session, sym))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))

    async def etf_holdings_for_tickers(
        self, 
        tickers: list[str], 
        pageSize: str = "200"
    ) -> dict[str, "ETFHoldings | None"]:
        """
        Fetch ETF holdings for multiple tickers concurrently.
        Returns {ticker: ETFHoldings or None}.
        """
        async with httpx.AsyncClient() as session:
            tasks = [
                asyncio.create_task(self._etf_holdings_single(session, sym, pageSize))
                for sym in tickers
            ]
            results = await asyncio.gather(*tasks)
        return dict(zip(tickers, results))
    


    async def fetch_latest_rsi(self, session: aiohttp.ClientSession, ticker: str, timespan: str = 'day') -> tuple[str, float | None, str | None]:
        """
        Fetch the latest RSI value for a single ticker.
        Returns a tuple of (ticker, latest_rsi_value, timestamp_in_et).
        If something goes wrong or no data is found, returns (ticker, None, None).
        """
        params = {
            "timespan": timespan,
            "adjusted": "true",
            "window": "14",
            "series_type": "close",
            "order": "desc",
            "limit": "100",      
            "apiKey": self.api_key
        }
        url = f"https://api.polygon.io/v1/indicators/rsi/{ticker}"
        
        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()  # Raise an exception for 4xx/5xx errors
                data = await response.json()
                results = data.get("results", {})
                values = results.get("values", [])
                if values:
                    latest_value = values[0]
                    rsi_value = latest_value["value"]
                    timestamp_utc = latest_value["timestamp"] / 1000  # Convert milliseconds to seconds
                    
                    # Convert timestamp to Eastern Time
                    utc_time = datetime.utcfromtimestamp(timestamp_utc).replace(tzinfo=pytz.UTC)
                    eastern_time = utc_time.astimezone(pytz.timezone("US/Eastern"))
                    formatted_time = eastern_time.strftime("%Y-%m-%d %H:%M:%S")
                    
                    return ticker, rsi_value, formatted_time
                else:
                    return ticker, None, None
        except Exception:
            # Handle network/API errors
            return ticker, None, None

    async def fetch_rsi_for_tickers(self, tickers: list[str], timespan: str = 'day') -> dict[str, dict]:
        """
        Fetch the latest RSI for multiple tickers concurrently.
        Returns a dictionary: { ticker: { 'rsi': latest_rsi_value_or_None, 'timestamp': timestamp_or_None } }.
        """
        async with aiohttp.ClientSession() as session:
            # Create a task for each ticker
            tasks = [
                asyncio.create_task(self.fetch_latest_rsi(session, ticker, timespan=timespan))
                for ticker in tickers
            ]
            # Run tasks concurrently
            results = await asyncio.gather(*tasks)
            # Convert list of tuples into a dictionary with detailed information
            return {
                ticker: {"rsi": rsi_value, "ts": timestamp}
                for ticker, rsi_value, timestamp in results
            }

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



    async def fetch_rsi_with_ema10_for_tickers(self, tickers: list[str], timespan: str) -> dict[str, float | None]:
        """
        Fetch the latest RSI (calculated with EMA10) for multiple tickers concurrently.
        Returns a dict: { ticker: latest_rsi_value_or_None }.
        """
        async with aiohttp.ClientSession() as session:
            results = {}

            for ticker in tickers:
                # Fetch price data for the current ticker
                try:
                    price_data = await poly.get_price(ticker)
                    
                    if price_data and len(price_data) >= 10:  # Ensure we have enough data
                        prices_series = pd.Series(price_data)  # Convert to a pandas Series
                        rsi_series = self.calculate_rsi_with_ema10(prices_series)  # Calculate RSI
                        results[ticker] = rsi_series.iloc[-1]  # Get the latest RSI value
                    else:
                        results[ticker] = None  # Not enough data
                except Exception as e:
                    print(f"Error fetching data for {ticker}: {e}")
                    results[ticker] = None  # Handle errors gracefully

            return results
        
    async def get_option_chain_all(
        self,
        underlying_asset: str,
        strike_price: float = None,
        strike_price_lte: float = None,
        strike_price_gte: float = None,
        expiration_date: str = None,
        expiration_date_gte: str = None,
        expiration_date_lte: str = None,
        contract_type: str = None,
        order: str = None,
        limit: int = 250,
        sort: str = None,
        insert: bool = False
    ):
        """
        Retrieve all options contracts for a specific underlying asset (ticker symbol) across multiple pages.
        """
        try:
            if not underlying_asset:
                raise ValueError("Underlying asset ticker symbol must be provided.")

            # Handle special case for index assets (e.g., "I:SPX" for S&P 500 Index)
            if underlying_asset.startswith("I:"):
                underlying_asset = underlying_asset.replace("I:", "")

            # Build query parameters
            params = {
                'strike_price': strike_price,
                'strike_price.lte': strike_price_lte,
                'strike_price.gte': strike_price_gte,
                'expiration_date': expiration_date,
                'expiration_date.gte': expiration_date_gte,
                'expiration_date.lte': expiration_date_lte,
                'contract_type': contract_type,
                'order': order,
                'limit': limit,
                'sort': sort
            }

            # Filter out None values
            params = {key: value for key, value in params.items() if value is not None}

            # Construct the API endpoint and query string
            endpoint = f"https://api.polygon.io/v3/snapshot/options/{underlying_asset}"
            if params:
                query_string = '&'.join([f"{key}={value}" for key, value in params.items()])
                endpoint += '?' + query_string
            endpoint += f"&apiKey={self.api_key}"

            logging.debug(f"Fetching option chain data for {underlying_asset} with query: {params}")

            # Fetch the data using asynchronous pagination
            response_data = await self.paginate_concurrent(endpoint)

            # Parse response data into a structured option snapshot object
            option_data = UniversalOptionSnapshot(response_data)

            # Insert into the database if specified
            if insert:
                logging.info("Inserting option chain data into the database.")
                await self.connect()  # Ensure connection to the database
                await self.batch_insert_dataframe(
                    option_data.df,
                    table_name='all_options',
                    unique_columns='option_symbol'
                )

            logging.info("Option chain data retrieval successful.")
            return option_data

        except ValueError as ve:
            logging.error(f"ValueError occurred: {ve}")
            return None
        except Exception as e:
            logging.error(f"An error occurred while fetching the option chain: {e}")
            return None
        

    async def check_macd_sentiment(self, hist: List[float]) -> Optional[str]:
        """
        Given a list of histogram values (most recent first),
        return 'bullish' or 'bearish' if conditions match,
        otherwise return None.
        """
        # Must have at least 3 data points
        if hist and len(hist) >= 3:
            # Extract the last three (most recent) values
            last_three_values = hist[:3]
            
            # Check bullish condition:
            #   1) The most recent histogram is near -0.02 (±0.04)
            #   2) The histogram values are strictly descending going backwards in time
            if (
                abs(last_three_values[0] - (-0.02)) < 0.04
                and all(last_three_values[i] > last_three_values[i + 1] for i in range(len(last_three_values) - 1))
            ):
                return 'bullish'

            # Check bearish condition:
            #   1) The most recent histogram is near 0.02 (±0.04)
            #   2) The histogram values are strictly ascending going backwards in time
            if (
                abs(last_three_values[0] - 0.02) < 0.04
                and all(last_three_values[i] < last_three_values[i + 1] for i in range(len(last_three_values) - 1))
            ):
                return 'bearish'

        # If no conditions are met, return None
        return None

    async def fetch_macd(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        timespan: str = 'day'
    ) -> Optional[Dict[str, Optional[str]]]:
        """
        Fetch MACD data for `ticker` and return a dictionary:
            {
                "sentiment": "bullish" or "bearish" or None,
                "timespan": timespan,
                "timestamp": "YYYY-MM-DD HH:MM:SS" (Eastern Time) or None
            }
        
        Returns None if any error occurs (e.g., network failure).
        """
        params = {
            "timespan": timespan,
            "adjusted": "true",
            "short_window": "12",
            "long_window": "26",
            "signal_window": "9",
            "series_type": "close",
            "order": "desc",    # newest data first
            "limit": "100",
            "apiKey": self.api_key
        }
        url = f"https://api.polygon.io/v1/indicators/macd/{ticker}"

        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()

            # If no results, bail
            results = data.get("results", {})
            if not results:
                return None

            macd_values = results.get("values", [])
            if not macd_values:
                return None

            # Build a DataFrame of MACD values
            df_macd = pd.DataFrame(macd_values)
            df_macd.rename(
                columns={
                    "histogram": "histogram",
                    "signal": "signal",
                    "timestamp": "timestamp",
                    "value": "macd",
                },
                inplace=True
            )
            # Sort so index=0 is the newest
            df_macd.sort_values("timestamp", ascending=False, inplace=True)
            df_macd.reset_index(drop=True, inplace=True)

            # Get the most recent timestamp and convert it to Eastern Time
            latest_timestamp_utc = df_macd.loc[0, "timestamp"] / 1000  # Convert milliseconds to seconds
            utc_time = datetime.utcfromtimestamp(latest_timestamp_utc).replace(tzinfo=pytz.UTC)
            eastern_time = utc_time.astimezone(pytz.timezone("US/Eastern"))
            formatted_timestamp = eastern_time.strftime("%Y-%m-%d %H:%M:%S")

            # Get histogram values (newest first)
            hist_values = df_macd["histogram"].tolist()

            # Determine sentiment
            sentiment = await self.check_macd_sentiment(hist_values)

            # Return a small dictionary with sentiment, timespan, and timestamp
            return {
                "sentiment": sentiment,  # "bullish", "bearish", or None
                "timespan": timespan,
                "timestamp": formatted_timestamp,
            }

        except Exception as e:
            print(f"Error fetching MACD data for {ticker}: {e}")
            return None


    async def fetch_macd_signals_for_tickers(
        self,
        tickers: List[str],
        timespan: str = 'day'
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """
        Fetch the MACD-based sentiment for multiple tickers concurrently.
        Returns a dict of the form:
            {
                "TICKER": {
                    "sentiment": "bullish" or "bearish" or None,
                    "timespan": "day" (for example),
                },
                ...
            }
        """
        async with aiohttp.ClientSession() as session:
            tasks = [
                asyncio.create_task(self.fetch_macd(session, t, timespan=timespan))
                for t in tickers
            ]
            results = await asyncio.gather(*tasks)

        # Map each ticker to its sentiment/timespan dictionary
        output: Dict[str, Dict[str, Optional[str]]] = {}
        for ticker, macd_info in zip(tickers, results):
            if macd_info is None:
                # If an error occurred or data didn't match, return None for sentiment
                output[ticker] = {
                    'ticker': ticker,
                    "sentiment": None,
                    "timespan": timespan
                }
            else:
                # Add the ticker to macd_info and assign it to the output
                output[ticker] = macd_info
                macd_info['ticker'] = ticker
                macd_info['timespan'] = timespan
                
        return output



    async def paginate_concurrent(self, url, as_dataframe=False, concurrency=250):
        """
        Concurrently paginates through polygon.io endpoints that contain the "next_url".
        """
        all_results = []
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
            return pd.DataFrame(all_results)
        else:
            return all_results
    def chunk_list(self, data, chunk_size):
        """Split a list into smaller chunks."""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]
    async def fetch_page(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()

    # ------------------------------------------------
    #  NEW: Multi-ticker concurrency
    # ------------------------------------------------
    async def get_option_chain_for_tickers(
        self,
        tickers: list[str],
        strike_price: float = None,
        strike_price_lte: float = None,
        strike_price_gte: float = None,
        expiration_date: str = None,
        expiration_date_gte: str = None,
        expiration_date_lte: str = None,
        contract_type: str = None,
        order: str = None,
        limit: int = 250,
        sort: str = None,
        insert: bool = False,
        concurrency: int = 75  # <-- Control your concurrency here
    ) -> dict[str, UniversalOptionSnapshot | None]:
        """
        Fetch option chain data for multiple tickers concurrently.
        Uses a semaphore to limit concurrency (instead of chunking).

        Returns:
            A dict mapping each ticker to either:
            - A UniversalOptionSnapshot object (if successful),
            - or None (if an error occurred).
        """
        out = {}

        # Define an inner async function that respects the semaphore
        async def fetch_with_semaphore(sem: asyncio.Semaphore, ticker: str):
            async with sem:
                try:
                    # Call your existing "get_option_chain_all" method
                    data = await self.get_option_chain_all(
                        underlying_asset=ticker,
                        strike_price=strike_price,
                        strike_price_lte=strike_price_lte,
                        strike_price_gte=strike_price_gte,
                        expiration_date=expiration_date,
                        expiration_date_gte=expiration_date_gte,
                        expiration_date_lte=expiration_date_lte,
                        contract_type=contract_type,
                        order=order,
                        limit=limit,
                        sort=sort,
                        insert=insert
                    )
                    return (ticker, data)
                except Exception as exc:
                    logging.error(f"Error fetching {ticker}: {exc}")
                    return (ticker, None)

        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency)

        # Kick off all tasks at once
        tasks = [
            asyncio.create_task(fetch_with_semaphore(semaphore, ticker))
            for ticker in tickers
        ]

        # Gather all results
        results = await asyncio.gather(*tasks)

        # Build the output dictionary
        for ticker, data in results:
            out[ticker] = data

        return out

    # ------------------------------------------------
    #  NEW: Multi-ticker concurrency
    # ------------------------------------------------
    async def get_atm_option_chain_for_tickers(
        self,
        tickers: list[str],
        limit: int = 250,
        insert: bool = False,
        concurrency: int = 75  # <-- Control your concurrency here
    ) -> dict[str, UniversalOptionSnapshot | None]:
        """
        Fetch option chain data for multiple tickers concurrently.
        Uses a semaphore to limit concurrency (instead of chunking).

        Returns:
            A dict mapping each ticker to either:
            - A UniversalOptionSnapshot object (if successful),
            - or None (if an error occurred).
        """
        out = {}

        # Define an inner async function that respects the semaphore
        async def fetch_with_semaphore(sem: asyncio.Semaphore, ticker: str):
            async with sem:
                try:
                    price = await poly.get_price(ticker)
                    strike_price_lte = price * 1.10
                    strike_price_gte = price * 0.90
                    expiration_date_gte = poly.today
                    expiration_date_lte = poly.fifteen_days_from_now

                    # Call your existing "get_option_chain_all" method
                    data = await self.get_option_chain_all(
                        underlying_asset=ticker,
                        strike_price_lte=strike_price_lte,
                        strike_price_gte=strike_price_gte,
                        expiration_date_gte=expiration_date_gte,
                        expiration_date_lte=expiration_date_lte,
                        limit=limit,
                        insert=insert
                    )
                    return (ticker, data)
                except Exception as exc:
                    logging.error(f"Error fetching {ticker}: {exc}")
                    return (ticker, None)

        # Create a semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency)

        # Kick off all tasks at once
        tasks = [
            asyncio.create_task(fetch_with_semaphore(semaphore, ticker))
            for ticker in tickers
        ]

        # Gather all results
        results = await asyncio.gather(*tasks)

        # Build the output dictionary
        for ticker, data in results:
            out[ticker] = data

        return out


    async def fetch_option_aggregates(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        multiplier: int,
        timespan: str,
        date_from: str,
        date_to: str,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 5000,
        max_retries: int = 5,
        initial_backoff: float = 1.0,
    ) -> pd.DataFrame:
        """
        Fetch aggregate bars from Polygon for a single option symbol.
        Returns a pandas DataFrame with parsed columns and converted timestamps.
        Includes simple retry logic with exponential backoff.
        """

        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/"
            f"{multiplier}/{timespan}/{date_from}/{date_to}"
            f"?adjusted={str(adjusted).lower()}"
            f"&sort={sort}"
            f"&limit={limit}"
            f"&apiKey={self.api_key}"
        )

        retries = 0
        backoff = initial_backoff

        # Acquire semaphore for concurrency control
        async with self.semaphore:
            while True:
                try:
                    async with session.get(url) as response:
                        data = await response.json()

                        # If Polygon responded with an error or unexpected status
                        if data.get("status") != "OK":
                            # If the results are empty, return an empty DataFrame
                            results = data.get("results", [])
                            if not results:
                                return pd.DataFrame()

                            # Otherwise, raise an error with the status
                            raise ValueError(
                                f"Request for {symbol} returned an unexpected status: "
                                f"{data.get('status')} => {data}"
                            )

                        # Parse data
                        results = data.get("results", [])
                        if not results:
                            # Return empty DataFrame if there are no results
                            return pd.DataFrame()

                        df = pd.DataFrame(results)

                        # Convert the Unix Msec timestamps in 't' to human-readable datetimes
                        # Polygon returns timestamps in milliseconds, so divide by 1000
                        df["t"] = pd.to_datetime(df["t"], unit="ms", utc=True)

                        # Rename columns to something more descriptive
                        df.rename(
                            columns={
                                "c": "close",
                                "h": "high",
                                "l": "low",
                                "n": "transactions",
                                "o": "open",
                                "t": "timestamp",
                                "v": "volume",
                                "vw": "vwap",
                            },
                            inplace=True,
                        )

                        # Set index to timestamp if you prefer time-series style DataFrames
                        df.set_index("timestamp", inplace=True, drop=True)

                        return df

                except (aiohttp.ClientError, ValueError) as e:
                    # In a real application, you'd handle specific error codes (e.g. 429 for rate limits).
                    retries += 1
                    if retries > max_retries:
                        print(f"[ERROR] Max retries reached for {symbol}. Error: {e}")
                        return pd.DataFrame()
                    else:
                        # Exponential backoff
                        sleep_time = backoff * (2 ** (retries - 1))
                        print(
                            f"[WARN] Error fetching data for {symbol} (retry {retries}/{max_retries}). "
                            f"Backing off {sleep_time:.2f}s. Error: {e}"
                        )
                        await asyncio.sleep(sleep_time)

    async def fetch_option_aggs_for_tickers(
        self,
        symbols: List[str],
        multiplier: int,
        timespan: str,
        date_from: str,
        date_to: str,
        adjusted: bool = True,
        sort: str = "asc",
        limit: int = 5000,
        batch_size: int = 50,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch aggregate bars for multiple option symbols concurrently in batches (optional).
        Returns a dictionary of DataFrames, keyed by symbol.

        :param symbols:     List of ticker symbols to fetch
        :param multiplier:  Multiplier for the time window
        :param timespan:    "second", "minute", "hour", "day", etc.
        :param date_from:   Start date (YYYY-MM-DD)
        :param date_to:     End date (YYYY-MM-DD)
        :param adjusted:    Whether to use adjusted data
        :param sort:        "asc" or "desc"
        :param limit:       Max records per request
        :param batch_size:  How many symbols to process per batch. If large, it can be all at once.
        """
        # Optionally, break the symbols list into chunks for better memory management
        def chunkify(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i : i + size]

        results_dict = {}

        async with aiohttp.ClientSession() as session:
            # Process symbols in batches to avoid overloading API or memory
            for batch_symbols in chunkify(symbols, batch_size):
                tasks = [
                    self.fetch_option_aggregates(
                        session,
                        symbol,
                        multiplier,
                        timespan,
                        date_from,
                        date_to,
                        adjusted,
                        sort,
                        limit,
                    )
                    for symbol in batch_symbols
                ]

                # Run tasks for the current batch concurrently
                batch_results = await asyncio.gather(*tasks)

                # Combine each DataFrame into a dictionary keyed by symbol
                for symbol, df in zip(batch_symbols, batch_results):
                    results_dict[symbol] = df

        return results_dict




    async def get_candle_data(
        self,
        ticker: str,
        interval: str,
        count: str = '800',
        timestamp: Optional[int] = None,
        client: Optional[httpx.AsyncClient] = None,
        headers=None,
    ) -> pd.DataFrame:
        """
        Fetch Webull candle data for a single ticker and interval.

        :param ticker: e.g. 'AAPL'
        :param interval: e.g. 'm5', 'm30', 'd', etc.
        :param count: number of candles, defaults to 800
        :param timestamp: optional override of current timestamp (Unix)
        :param client: optional shared httpx.AsyncClient
        :param headers: optional HTTP headers
        :return: pandas DataFrame with columns: Timestamp, Open, Close, High, Low, Volume, etc.
        """
        async with self.semaphore:
            try:
                # Adjust ticker if needed
                original_ticker = ticker
                if ticker == 'I:SPX':
                    ticker = 'SPX'
                elif ticker == 'I:NDX':
                    ticker = 'NDX'
                elif ticker == 'I:VIX':
                    ticker = 'VIX'
                elif ticker == 'I:RUT':
                    ticker = 'RUT'
                elif ticker == 'I:XSP':
                    ticker = 'XSP'

                if timestamp is None:
                    timestamp = int(time.time())

                tickerid = await self.get_webull_id(ticker)  # presumably an async function

                base_fintech_gw_url = (
                    f'https://quotes-gw.webullfintech.com/api/quote/charts/query-mini'
                    f'?tickerId={tickerid}&type={interval}&count={count}'
                    f'&timestamp={timestamp}&restorationType=1&extendTrading=0'
                )
                print(base_fintech_gw_url)

                # With httpx, just do: response = await client.get(...)
                response = await client.get(base_fintech_gw_url, headers=headers)
                response.raise_for_status()
                data_json = response.json()

                # The returned JSON often looks like: [ { "data": [...], ... } ]
                if data_json and isinstance(data_json, list) and 'data' in data_json[0]:
                    raw_data = data_json[0]['data']
                    split_data = [row.split(",") for row in raw_data]
                    df = pd.DataFrame(
                        split_data,
                        columns=['ts', 'o', 'c', 'h', 'l', 'vwap', 'v', 'a']
                    )

                    # Convert 'ts' to datetime
                    df['ts'] = pd.to_numeric(df['ts'], errors='coerce')
                    df['ts'] = pd.to_datetime(df['ts'], unit='s', utc=True)
                    df['ts'] = df['ts'].dt.tz_convert('US/Eastern').dt.tz_localize(None)

                    df['ticker'] = original_ticker
                    df['ts'] = df['ts'].dt.strftime('%Y-%m-%dT%H:%M:%S')

                    # Reverse order so earliest date is first, if that's what you want
                    df = df.iloc[::-1].reset_index(drop=True)

                    numeric_cols = ['o', 'c', 'h', 'l', 'v', 'vwap']
                    for col in numeric_cols:
                        df[col] = df[col].astype(float)

                    df = df.drop(columns=['a'])
                    return df

                return pd.DataFrame()

            except Exception as e:
                print(f"Error fetching data for {ticker} ({interval}): {e}")
                return pd.DataFrame()

    async def get_candle_data_for_tickers_and_intervals(
        self,
        tickers: List[str],
        intervals: List[str],
        count: str = '800',
        headers=None
    ) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Fetch candle data for multiple tickers and multiple intervals concurrently.
        Returns a nested dict: { ticker: { interval: DataFrame } }.

        :param tickers: list of ticker strings
        :param intervals: list of intervals, e.g. ['m5', 'd']
        :param count: number of candles
        :param headers: optional HTTP headers
        :return: { ticker: { interval: DataFrame } }
        """
        results = {}
        async with httpx.AsyncClient(headers=headers) as client:
            tasks = []
            task_info = []
            for ticker in tickers:
                for interval in intervals:
                    task = self.get_candle_data(
                        ticker=ticker,
                        interval=interval,
                        count=count,
                        client=client,      # pass the httpx.AsyncClient
                        headers=headers
                    )
                    tasks.append(task)
                    task_info.append((ticker, interval))

            fetched_data = await asyncio.gather(*tasks, return_exceptions=True)

        for (ticker, interval), df in zip(task_info, fetched_data):
            if ticker not in results:
                results[ticker] = {}
            if isinstance(df, Exception):
                print(f"Failed to fetch data for {ticker} @ {interval}: {df}")
                results[ticker][interval] = pd.DataFrame()
            else:
                results[ticker][interval] = df

        return results

    async def get_candle_data_for_tickers(
            self,
            tickers: List[str],
            interval: str,
            count: str = '800',
            headers=None
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch candle data for multiple tickers concurrently using a shared HTTP session.
        Returns a dictionary { ticker: DataFrame }.

        :param tickers: list of ticker strings
        :param interval: e.g. 'm5', 'm30', 'd', etc.
        :param count: number of candles
        :param cache_ttl: time to live in seconds for the cache
        """
        results = {}
        async with httpx.AsyncClient(headers=headers) as client:
            tasks = [
                self.get_candle_data(
                    ticker=ticker,
                    interval=interval,
                    count=count,
                    client=client,
                    headers=headers
                )
                for ticker in tickers
            ]
            fetched_data = await asyncio.gather(*tasks, return_exceptions=True)

        for ticker, df in zip(tickers, fetched_data):
            if isinstance(df, Exception):
                print(f"Failed to fetch data for {ticker}: {df}")
                results[ticker] = pd.DataFrame()
            else:
                results[ticker] = df

        return results
    

    async def fetch_option_trades(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        start_timestamp: str = None,
        end_timestamp: str = None,
        order: str = "asc",
        sort: str = "sip_timestamp",
        limit: int = 1000,
    ) -> pd.DataFrame:
        """
        Fetch trade data for a single option symbol from Polygon's v3/trades endpoint,
        then add a 'notional_value' column, as well as single-row flags for
        lowest/highest trade prices (and timestamps) plus highest trade size (and timestamp).
        """
        # Base URL for the trades endpoint
        base_url = f"https://api.polygon.io/v3/trades/{symbol}"

        # Build initial query params
        params = {
            "limit": limit,
            "apiKey": self.api_key,
            "order": order,
            "sort": sort,
        }
        if start_timestamp:
            params["timestamp.gte"] = start_timestamp
        if end_timestamp:
            params["timestamp.lte"] = end_timestamp

        all_results = []
        next_url = None

        while True:
            if next_url:
                url = next_url
                query_params = {}
            else:
                url = base_url
                query_params = params

            async with session.get(url, params=query_params) as response:
                data = await response.json()

            status = data.get("status", None)
            if status != "OK":
                raise ValueError(
                    f"Request for {symbol} returned an unexpected status: {status}"
                )

            results = data.get("results", [])
            if not results:
                # No trades found or no more pages
                break

            all_results.extend(results)
            next_url = data.get("next_url", None)
            if next_url is not None:
                next_url += f'&{urlencode({"apiKey": f"{self.api_key}"})}'
            if not next_url:
                break

        if not all_results:
            # Return an empty DataFrame if no trades
            return pd.DataFrame()

        # Convert results -> DataFrame
        df = pd.DataFrame(all_results)
        df["option_symbol"] = symbol

        # Extract human-readable metadata
        components = get_human_readable_string(symbol)
        strike = components.get("strike_price")
        expiry = components.get("expiry_date")
        cp = components.get("call_put")
        ticker = components.get("underlying_symbol")

        df["ticker"] = ticker
        df["strike"] = strike
        df["call_put"] = cp
        df["expiry"] = expiry

        # Convert sip_timestamp -> string format (if it exists)
        if "sip_timestamp" in df.columns:
            df["sip_timestamp"] = pd.to_datetime(
                df["sip_timestamp"], unit="ns", utc=True
            ).dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")  # Convert to string format

        # Rename for clarity
        df.rename(
            columns={
                "sip_timestamp": "sip_datetime",
            },
            inplace=True,
        )

        # --------------------------------------------------------------------
        # 1) Compute the notional value per trade:
        #     notional = trade_size * (price * 100)
        # --------------------------------------------------------------------
        if {"size", "price"}.issubset(df.columns):
            df["notional_value"] = df["size"] * (df["price"] * 100.0)
        else:
            df["notional_value"] = None

        # --------------------------------------------------------------------
        # 2) Convert "conditions" from [int] -> int -> label (if present)
        # --------------------------------------------------------------------
        if "conditions" in df.columns:
            df["condition_code"] = df["conditions"].apply(
                lambda arr: arr[0] if isinstance(arr, list) and len(arr) > 0 else None
            )
            df["condition_label"] = df["condition_code"].map(option_condition_dict)

        # --------------------------------------------------------------------
        # 3) Convert "exchange" from int -> label (if present)
        # --------------------------------------------------------------------
        if "exchange" in df.columns:
            df["exchange_label"] = df["exchange"].map(OPTIONS_EXCHANGES)

        # --------------------------------------------------------------------
        # 4) Compute and flag the single row for lowest/highest price and highest trade size
        # --------------------------------------------------------------------
        if "price" in df.columns:
            lowest_price = df["price"].min()
            highest_price = df["price"].max()

            lowest_price_idx = df["price"].idxmin()
            highest_price_idx = df["price"].idxmax()

            lowest_price_ts = (
                df.loc[lowest_price_idx, "sip_datetime"]
                if pd.notnull(lowest_price_idx)
                else None
            )
            highest_price_ts = (
                df.loc[highest_price_idx, "sip_datetime"]
                if pd.notnull(highest_price_idx)
                else None
            )

            # Create columns filled with None
            df["lowest_price"] = None
            df["lowest_price_timestamp"] = None
            df["highest_price"] = None
            df["highest_price_timestamp"] = None

            # Overwrite only the row where min / max price occurs
            if pd.notnull(lowest_price_idx):
                df.loc[lowest_price_idx, "lowest_price"] = lowest_price
                df.loc[lowest_price_idx, "lowest_price_timestamp"] = lowest_price_ts
            if pd.notnull(highest_price_idx):
                df.loc[highest_price_idx, "highest_price"] = highest_price
                df.loc[highest_price_idx, "highest_price_timestamp"] = highest_price_ts

        if "size" in df.columns:
            highest_size = df["size"].max()
            highest_size_idx = df["size"].idxmax()
            highest_size_ts = (
                df.loc[highest_size_idx, "sip_datetime"]
                if pd.notnull(highest_size_idx)
                else None
            )

            # Create columns filled with None
            df["highest_trade_size"] = None
            df["highest_trade_timestamp"] = None

            # Overwrite only the row where the largest trade size occurs
            if pd.notnull(highest_size_idx):
                df.loc[highest_size_idx, "highest_trade_size"] = highest_size
                df.loc[highest_size_idx, "highest_trade_timestamp"] = highest_size_ts

        return df

    async def fetch_option_trades_for_tickers(
        self,
        symbols: list,
        start_timestamp: str = None,
        end_timestamp: str = None,
        order: str = "asc",
        sort: str = "sip_timestamp",
        limit: int = 1000,
    ) -> dict:
        """
        Concurrently fetch trade data for multiple option symbols.
        Returns a dict of DataFrames keyed by symbol. Each DataFrame includes
        the notional_value and single-row min/max flags as computed in fetch_option_trades().
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for symbol in symbols:
                task = self.fetch_option_trades(
                    session,
                    symbol,
                    start_timestamp,
                    end_timestamp,
                    order,
                    sort,
                    limit,
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks)

        return {symbol: df for symbol, df in zip(symbols, results)}
    
    async def fetch_ticker_news(
        self,
        session: aiohttp.ClientSession,
        ticker: str,
        headers=None,
    ) -> pd.DataFrame:
        """
        Fetch up to 1000 recent news articles for a single ticker from Polygon's
        /v2/reference/news endpoint (one request, no pagination).

        Normalizes nested data:
          - Extracts publisher fields into top-level columns
          - Extracts the first item of `insights` into columns
          - Joins 'keywords' and 'tickers' into comma-separated strings
        """
        ticker_id = self.ticker_to_id_map.get(ticker)
        try:
            async with session.get(f"https://nacomm.webullfintech.com/api/information/news/tickerNewses/v9?tickerId={ticker_id}&pageSize=50&showAiNews=true", headers=headers) as resp:

                data = await resp.json()

                news_df = pd.DataFrame(data)

                if 'ranks' in news_df.columns:
                    news_df = news_df.drop(columns=['ranks'])
                return news_df
        except Exception as e:
            print(e)

    async def fetch_ticker_news_for_symbols(
        self,
        symbols: list,
        headers=None,
    ) -> dict:
        """
        Fetch up to 1000 recent news articles concurrently for multiple tickers
        from Polygon's /v2/reference/news endpoint (one request per symbol, no pagination).

        Normalizes nested data similarly to fetch_ticker_news.

        Returns
        -------
        dict : {ticker: pd.DataFrame}
        """
        async with aiohttp.ClientSession() as session:
            tasks = []
            for sym in symbols:
                task = self.fetch_ticker_news(
                    session,
                    sym,
                    headers=headers
                )
                tasks.append(task)

            # Gather all tasks (run them concurrently)
            results = await asyncio.gather(*tasks)

        # Combine each DataFrame into a dict keyed by the symbol
        return {symbol: df for symbol, df in zip(symbols, results)}
    

    async def fetch_atm_vol_analysis(self, ticker):
        # 1. Query the database
        await db.connect()
        try:
            price = await poly.get_price(ticker)

            lower_strike = price * 0.70
            upper_strike = price * 1.30

            query = f"""
                SELECT *
                FROM option_symbols
                WHERE ticker = '{ticker}'
                AND strike >= {lower_strike}
                AND strike <= {upper_strike}
                AND expiry <= CURRENT_DATE + INTERVAL '10 days';
            """

            results = await db.fetch(query)
            columns = await db.get_table_columns(table='option_symbols')
            df = pd.DataFrame(results, columns=columns)

            # 2. Build a list of tasks for concurrency
            tasks = []
            async with httpx.AsyncClient() as client:
                for row in df.itertuples():
                    tasks.append(
                        asyncio.create_task(
                            self.fetch_volume_analysis(client, row.option_id)
                        )
                    )

                # 3. Run all tasks concurrently
                responses = await asyncio.gather(*tasks)

            # 4. Merge the returned data back into df
            #    Convert the list of responses into a dict keyed by option_id
            response_dict = {}
            for resp in responses:
                if not resp:
                    continue
                option_id = resp.get("option_id")
                response_dict[option_id] = resp

            # Create columns for the main Webull fields
            df["ticker_id"] = None
            df["total_volume"] = None
            df["avg_price"] = None
            df["buy_volume"] = None
            df["sell_volume"] = None
            df["neutral_volume"] = None
            df["wb_dates"] = None    # initially store the raw array
            df["wb_datas"] = None    # store the raw list of volume objects

            for idx, row in df.iterrows():
                resp = response_dict.get(row["option_id"])
                if resp:
                    df.at[idx, "ticker_id"] = resp.get("tickerId")
                    df.at[idx, "total_volume"] = resp.get("totalVolume")
                    df.at[idx, "avg_price"] = resp.get("avgPrice")
                    df.at[idx, "buy_volume"] = resp.get("buyVolume")
                    df.at[idx, "sell_volume"] = resp.get("sellVolume")
                    df.at[idx, "neutral_volume"] = resp.get("neutralVolume")
                    df.at[idx, "wb_dates"] = resp.get("dates")
                    df.at[idx, "wb_datas"] = resp.get("datas")

            # 5. Sum up all nested buy/sell in each row’s wb_datas
            #    and add those sums to wb_buyVolume / wb_sellVolume (or replace them).
            for idx, row in df.iterrows():
                # Only process if 'wb_datas' is not None (or empty)
                if row["wb_datas"]:
                    # Sum up the nested fields
                    nested_buy = sum(d.get("buy", 0) for d in row["wb_datas"])
                    nested_sell = sum(d.get("sell", 0) for d in row["wb_datas"])
                    nested_neutral = sum(
                        # There's no direct "neutral" field in each item,
                        # but if you need it, sum similarly.
                        0 for d in row["wb_datas"]
                    )
                    # Add them to existing volumes
                    df.at[idx, "buy_volume"] = (row["buy_volume"] or 0) + nested_buy
                    df.at[idx, "sell_volume"] = (row["sell_volume"] or 0) + nested_sell
                    df.at[idx, "neutral_volume"] = (row["neutral_volume"] or 0) + nested_neutral

            # 6. Drop the columns you don’t want
            #    (If you only want to hide them, you can skip the drop step.)
            df.drop(["wb_dates", "wb_datas"], axis=1, inplace=True)
            df['buy_volume'] = df['buy_volume'].astype(float)
            df['neutral_volume'] = df['neutral_volume'].astype(float)
            df['sell_volume'] = df['sell_volume'].astype(float)
            df['total_volume'] = df['total_volume'].astype(float)
            df['avg_price'] = df['avg_price'].astype(float)
            return df
        except Exception as e:
            print(e)

    async def fetch_volume_analysis(self, client: httpx.AsyncClient, option_id: int, headers=None):
        """
        Makes one HTTP request to Webull for the given option_id,
        returns the parsed JSON with 'option_id' included for
        merging back into the DataFrame.
        """
        try:
            r = await client.get(
                f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=800&tickerId={option_id}",
                headers=headers,
                timeout=10.0
            )
            data = r.json()
            data["option_id"] = option_id
            return data
        except Exception as e:
            print(f"Error fetching volume analysis for option_id={option_id}: {e}")
            return None

    async def make_POC_for_tickers(self, tickers, interval, count):
        """
        Asynchronously fetch minute-level candle data, compute a naive volume profile,
        and return the POC line, its percent difference from the latest close,
        and whether it's above or below the latest price in the DataFrame.

        :param ticker: The stock ticker symbol to retrieve data for.
        :return: A dictionary containing:
            {
                'POC': float,
                'pct_diff': float,
                'above_below': str
            }
        """
        # 1) Retrieve minute-level candle data:
        #    Note: We assume get_candle_data_for_tickers(tickers, interval) returns a pandas DataFrame
        #    with columns: [Timestamp, Open, Close, High, Low, Vwap, Volume, Avg, Ticker, timespan]
        candle_dataframe = await self.get_candle_data_for_tickers(
            tickers=tickers,
            interval=interval, count=count
        )
        
        all_dataframes = []
        for ticker, dataframe in candle_dataframe.items():
            print(candle_dataframe)
            if dataframe is not None:

                print(dataframe)
                # 2) Define bins for volume profile:
                min_price = dataframe['l'].min()
                max_price = dataframe['h'].max()
                
                # Adjust the number of bins if needed (50 is just an example):
                number_of_bins = 12
                bins = np.linspace(min_price, max_price, number_of_bins)

                # Prepare a holder for the aggregated volume in each bin
                volume_profile = np.zeros(len(bins) - 1)

                # 3) Populate the volume_profile by distributing each candle's volume 
                #    to the bin corresponding to that candle's VWAP (naive approach).
                for _, row in dataframe.iterrows():
                    vwap_value = row['vwap']
                    vol = row['v']

                    # Find which bin this VWAP belongs to
                    bin_index = np.digitize(vwap_value, bins) - 1

                    # Ensure bin_index is in range
                    if 0 <= bin_index < len(volume_profile):
                        volume_profile[bin_index] += vol

                # 4) Identify the bin with the highest aggregated volume (Point of Control - POC)
                poc_bin_index = volume_profile.argmax()

                # We'll define the POC as the midpoint of the bin with the greatest volume
                poc_low_bound = bins[poc_bin_index]
                poc_high_bound = bins[poc_bin_index + 1]
                poc_price = (poc_low_bound + poc_high_bound) / 2

                # 5) Determine the latest close price in the DataFrame
                latest_price = dataframe.iloc[0]['c']

                # 6) Calculate the % difference (POC vs. latest close)
                pct_diff = ((poc_price - latest_price) / latest_price) * 100

                # 7) Determine if the POC is above or below the latest close
                above_below = "above" if poc_price > latest_price else "below"

                # Return ONLY the requested information
                dict= {
                    'ticker': ticker,
                    'POC': float(poc_price),
                    'pct_diff': float(pct_diff),
                    'above_below': above_below,
                    'num_candles': count,
                }
                all_dataframes.append(dict)
        return pd.DataFrame(all_dataframes)
    

    def td_sequential_setup(self, df, price_col="close", time_col="timestamp"):
        """
        Build the 'setup' count portion of TD Sequential:
        - Positive counts (1..9) => potential buy setup
        - Negative counts (-1..-9) => potential sell setup
        - 0 => no active setup at that bar

        Officially:
        - A "buy setup" starts at bar #1 if (close[i] < close[i-4]).
        - Each consecutive bar is #2..#9 if (close[i] < close[i-4]).
        - A "sell setup" starts at bar #1 if (close[i] > close[i-4]).
        - Each consecutive bar is #2..#9 if (close[i] > close[i-4]).

        IMPORTANT CHANGE:
        - Once the count reaches ±9, we STOP counting and reset.
            (No bars with values beyond ±9.)
        """
        df = df.sort_values(by=time_col).reset_index(drop=True)

        setup_counts = [0] * len(df)
        direction = None  # 'buy' or 'sell'
        count = 0

        for i in range(len(df)):
            if i < 4:
                # We need at least 4 prior bars to compare
                setup_counts[i] = 0
                continue

            close_now = df.loc[i, price_col]
            close_4ago = df.loc[i - 4, price_col]

            if direction is None or count == 0:
                # Attempt to start a new sequence
                if close_now < close_4ago:
                    direction = "buy"
                    count = 1
                    setup_counts[i] = count
                elif close_now > close_4ago:
                    direction = "sell"
                    count = 1
                    setup_counts[i] = -count
                else:
                    direction = None
                    count = 0
                    setup_counts[i] = 0

            else:
                # We already have an active setup in progress
                if direction == "buy":
                    if close_now < close_4ago:
                        count += 1
                        # Cap at 9, then reset
                        if count > 9:
                            count = 9
                            direction = None
                        setup_counts[i] = count
                    else:
                        # Flip or break
                        if close_now > close_4ago:
                            direction = "sell"
                            count = 1
                            setup_counts[i] = -count
                        else:
                            direction = None
                            count = 0
                            setup_counts[i] = 0

                elif direction == "sell":
                    if close_now > close_4ago:
                        count += 1
                        # Cap at 9, then reset
                        if count > 9:
                            count = 9
                            direction = None
                        setup_counts[i] = -count
                    else:
                        # Flip or break
                        if close_now < close_4ago:
                            direction = "buy"
                            count = 1
                            setup_counts[i] = count
                        else:
                            direction = None
                            count = 0
                            setup_counts[i] = 0

        df["td_setup_count"] = setup_counts
        return df


    def check_td9(self, df, price_col="close", time_col="timestamp"):
        """
        Check if the last bar is exactly bar #9 (positive or negative).
        - +9 => "CURRENT TD9 BUY"
        - -9 => "CURRENT TD9 SELL"
        - otherwise => None
        """
        df_with_counts = self.td_sequential_setup(df, price_col, time_col)
        last_count = df_with_counts.iloc[-1]["td_setup_count"]

        if last_count == 9:
            return "CURRENT TD9 BUY"
        elif last_count == -9:
            return "CURRENT TD9 SELL"
        else:
            return None


    async def check_candles_for_pretd9(self, ticker, timespan, headers=None):
        """
        1) Pull the latest 14 candles for the given ticker/timespan.
        2) Check if the final bar is #9 (buy or sell).
        3) If so, upsert the result into table "td9_pre".
        """
        try:
            df = await ta.get_candle_data(ticker, interval=timespan, headers=headers, count='800')
            df = df.rename(columns={'Close': 'close', 'Avg': 'avg', 'Low': 'low', 'High': 'high', 'Open': 'open', 'Volume': 'volume', 'Vwap': 'vwap', 'Timestamp': 'ts'})
            df.sort_values(by='ts', inplace=True)

            # Check if last bar is #9
            potential_signal = self.check_td9(df, price_col='close', time_col='ts')
            if potential_signal is not None:
                final_ts = df['ts'].iloc[-1]
                final_setup_count = self.td_sequential_setup(df, 'close', 'ts').iloc[-1]['td_setup_count']

                row_to_store = pd.DataFrame([{
                    'ticker': ticker,
                    'timespan': timespan,
                    'ts': final_ts,
                    'td_setup_count': final_setup_count,
                    'td_signal': potential_signal
                }])

                # Upsert to table "td9_pre" (adjust table name / columns if needed)
                await db.batch_upsert_dataframe(
                    row_to_store,
                    table_name='live_td9',
                    unique_columns=['ticker', 'timespan', 'ts']
                )
            print(potential_signal)
            return potential_signal
        except Exception as e:
            print(e)


    def chunk_list(self, data, chunk_size):
        """
        Helper to chunk a list into consecutive pieces.
        """
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]