import os
from dotenv import load_dotenv
import json
from io import StringIO
import re
import requests
import tempfile
load_dotenv
import aiohttp
import asyncio
import pandas as pd
import aiofiles
import httpx
from scipy.stats import norm
from typing import Optional
from .occ_models import OptionsMonitor, OICOptionsMonitor, VolaSnapshot,AllCompBoundVola, HistoricIVX, VolatilityScale, CalculatorMetrics
from .occ_models import OCCMostActive, StockInfo, ExpiryDates, ProbabilityMetrics, ExpiryDates, CalculateGreeks
from .occ_models import StockLoans, VolumeTotals, DailyMarketShare
from .occ_models import NEWOIC
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore", message="Precision loss occurred in moment calculation due to catastrophic cancellation")
from .occ_models import flatten_json
from fudstop4.apis.helpers import format_large_numbers_in_dataframe
import math
from typing import List, Dict
from asyncpg import create_pool
webull_id_df = pd.read_csv('files/ticker_csv.csv')
ticker_df = pd.read_csv('files/occ_tickers.csv')
def make_unique(columns):
    """
    Given a list of column names, appends an underscore and a number to any duplicates
    to ensure all column names are unique.
    """
    seen = {}
    unique_cols = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            unique_cols.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            unique_cols.append(col)
    return unique_cols

class occSDK:
    def __init__(self, host: str = 'localhost', port: int = 5432, user: str = 'chuck',
                 password: str = 'fud', database: str = 'fudstop3', webull_id_df=None, ticker_df=None):

        self.conn = None
        self.pool = None
        self.session = None
        self.client_key = os.environ.get('OCC_CLIENT')
        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.occ_tickers = pd.read_csv('files/occ_tickers.csv')
        self.occ_ticker_df = dict(zip(self.occ_tickers['symbol'], self.occ_tickers['id']))
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))
        self.webull_id_df = webull_id_df
        self.ticker_df = ticker_df
        self.host = host
        self.port = port
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.user = user
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.today_mmddyyyy = datetime.now().strftime('%m/%d/%Y')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.password = password
        self.database = database
        self.connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        self.base_url = f"https://marketdata.theocc.com/mdapi/"
        self.chat_memory = []  # In-memory list to store chat messages


    @staticmethod
    def load_ticker_to_id():
        # Read the CSV file into a DataFrame
        df = pd.read_csv('files/occ_tickers.csv')
        # Create a dictionary mapping from the 'symbol' column to the 'id' column
        return pd.Series(df.id.values, index=df.symbol).to_dict()

    # Fetch all URLs
    async def get_headers(self):
        try:
            """REFRESH BEARER TOKEN"""    
            payload = { 
                'clientKey': self.client_key,
                'clientName': 'OIC_test',
                'userIdenteficator': os.environ.get('userIdenteficator')
            }
            async with httpx.AsyncClient() as client:
                resp = await client.post("https://ivlivefront.ivolatility.com/token/client/get", json=payload)

                token = resp.text
                headers = { 
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                    'Authorization': f'Bearer {token}',
                    'Accept': 'application/json, text/plain, */*'
                }
                return headers
        except Exception as e:
            print(e)

    async def convert_ms_timestamp(self, timestamp_ms):
        # Convert milliseconds to seconds
        timestamp_s = timestamp_ms / 1000.0
        return datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
    async def fetch_url(session, url):
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Error: {response.status}")
                return None
    async def get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def connect(self):
        self.pool = await create_pool(
            host=self.host, pool=self.pool, password=self.password, database=self.database, port=self.port, min_size=1, max_size=40
        )

        return self.pool
        




    async def save_to_database(self, flattened_data):
        async with self.pool.acquire() as conn:
            # Flatten the data and prepare it for insertion

            
            # Prepare the SQL INSERT query
            columns = ', '.join(flattened_data.keys())
            placeholders = ', '.join([f'${i+1}' for i in range(len(flattened_data))])
            query = f'INSERT INTO occ_totals ({columns}) VALUES ({placeholders})'
            
            # Execute the query
            await conn.execute(query, *flattened_data.values())


    async def get_rate(self, ticker):
        try:
            url = f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/parameters?stockid={ticker.T}&isEod=false"
        except Exception as e:
            print(e)
            url = f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/parameters?stockid={await self.get_stock_id}&isEod=false"
        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=await self.get_headers())
            
            if data.status_code == 200:
                data = data.json()
                return ProbabilityMetrics(data)

    async def get_expirations(self, ticker):
        stock_id = await self.get_stock_id(ticker)
        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://private-authorization.ivolatility.com/optcalc/dictionary/expiration/{stock_id}?isEod=false")

            if data.status_code == 200:
                data = data.json()

                return ExpiryDates(data)


    async def stock_loans(self, report_date: str = None, type: str = "daily", as_dataframe:bool=True) -> Optional[StockLoans]:
        """Retrieve stock loan data for a specific report date and type.

        Args:
            report_date (str): Report date in YYYY-MM-DD format. Defaults to today's date.
            type (str): Report type. Defaults to "daily".

        Returns:
            Optional[StockLoans]: Stock loan data for the specified report date and type, or None if data is not available.
        """
        if report_date is None:
            report_date = self.today
        url=f"https://marketdata.theocc.com/mdapi/stock-loan?report_date={report_date}&report_type={type}"
        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            r = data.json()
            entity = r['entity']
   
            stockLoanResults = [i.get('stockLoanResults') for i in entity]
            stockLoanResults = StockLoans(stockLoanResults)
            if as_dataframe == True:
                df = stockLoanResults.as_dataframe
                df = format_large_numbers_in_dataframe(df)
                
                return df
            if stockLoanResults:
                return stockLoanResults
            else:
                return None



    async def volume_totals(self) -> VolumeTotals:
        """Retrieve volume totals data.

        Returns:
            VolumeTotals: Volume totals data.
        """
        url = "https://marketdata.theocc.com/mdapi/volume-totals"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            r = data.json()
            entity = r['entity']
            if entity is not None:

                data = flatten_json(entity)

                df = pd.DataFrame(data, index=[0])

                df = format_large_numbers_in_dataframe(df)
                return df
            

    async def open_interest(self):

        """
        
        DATE FORMAT = MM/DD/YYYY
        """
        url = f"https://marketdata.theocc.com/mdapi/open-interest?report_date={self.today_mmddyyyy}"
        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            r = data.json()

            entity = r['entity']


            optionsOI = entity['optionsOI']
            all_data = []

            for i in optionsOI:
                activityDate = await self.convert_ms_timestamp(i.get('activityDate'))
                equityCalls = i.get('equityCalls')
                equityPuts = i.get('equityPuts')
                indexCalls = i.get('indexCalls')
                indexPuts = i.get('indexPuts')
                treasuryCalls = i.get('treasuryCalls')
                treasuryPuts = i.get('treasuryPuts')
                equityTotal = i.get('equityTotal')
                indexTotal = i.get('indexTotal')
                treasuryTotal = i.get('treasuryTotal')
                futuresTotal = i.get('futuresTotal')
                occTotal = i.get('occTotal')

                data_dict = { 

                    'activity_date': activityDate,
                    'equity_calls': equityCalls,
                    'equity_puts': equityPuts,
                    'equity_total': equityTotal,
                    'index_calls': indexCalls,
                    'index_puts': indexPuts,
                    'index_total': indexTotal,
                    'treasury_calls': treasuryCalls,
                    'treasury_puts': treasuryPuts,
                    'treasury_total': treasuryTotal,
                    'futures_total': futuresTotal,
                    'occTotal': occTotal
                }

                all_data.append(data_dict)


            df =pd.DataFrame(all_data)
            df = format_large_numbers_in_dataframe(df)
            return df


    async def daily_market_share(self, date=None):
        today_str = pd.Timestamp.today().strftime('%Y-%m-%d')
        date = today_str if not date else date
        url = f"https://marketdata.theocc.com/mdapi/daily-volume-totals?report_date={date}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            r = data.json()
            entity = r['entity']
            if entity['total_volume'] == []:
                f"https://marketdata.theocc.com/mdapi/daily-volume-totals?report_date={self.yesterday}"
                entity = r['entity']
                

                total_volume = DailyMarketShare(entity)
                df = total_volume.df
                
                df = format_large_numbers_in_dataframe(df)
                
                return df


            

            else:

                total_volume = DailyMarketShare(entity)
                df = total_volume.df
                df['date'] = date
                df = format_large_numbers_in_dataframe(df)
                return df

    async def fetch_most_active(self):
        async with httpx.AsyncClient() as client:
            headers = await self.get_headers()
            response = await client.get("https://private-authorization.ivolatility.com/favorites/instruments/most-active", headers=headers)
            return OCCMostActive(response.json())

    async def get_webull_id(self, symbol):
        """
        Converts ticker name to ticker ID to be passed to other API endpoints from Webull.
        Ensures the 'files/' directory exists before accessing the file.
        """
        try:
            webull_id_df = pd.read_csv('files/ticker_csv.csv')  # Read the CSV
            ticker_id = webull_id_df.loc[webull_id_df['ticker'] == symbol, 'id'].values[0]
            return ticker_id
        except FileNotFoundError:
            print(f"Error: The file 'files/ticker_csv.csv' does not exist.")
            return None
        except IndexError:
            print(f"No ID found for ticker: {symbol}")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    

    async def get_stock_id(self, ticker):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        try:
            stock_id = ticker_df.loc[ticker_df['symbol'] == ticker, 'id'].values[0]
            return stock_id
        except Exception as e:
            print(e)

        
    async def get_center(self, symbol):
        ticker_id = await self.get_webull_id(symbol)
        async with httpx.AsyncClient() as client:   

            data = await client.get(f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count=10&tickerId={ticker_id}&type=0")
            print(data)
            data = data.json()
            avg_price = data['avePrice']

            return avg_price



    async def options_monitor(self, symbol: str):
        """
        Fetch options data for a single ticker (as you defined).
        """
        try:
            stockId = await self.get_stock_id(symbol)
            center = await self.get_center(symbol)
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"https://private-authorization.ivolatility.com/options-monitor/listOptionDataRow?"
                    f"stockId={stockId}&regionId=1&center={center}&"
                    "columns=alpha&columns=ask&columns=asksize&columns=asktime&columns=bid&"
                    "columns=bidsize&columns=bidtime&columns=change&columns=changepercent&columns=delta&"
                    "columns=theoprice&columns=gamma&columns=ivint&columns=ivask&columns=ivbid&columns=mean&"
                    "columns=openinterest_eod&columns=optionsymbol&columns=volume&columns=paramvolapercent_eod&"
                    "columns=alpha_eod&columns=ask_eod&columns=bid_eod&columns=delta_eod&columns=theoprice_eod&"
                    "columns=gamma_eod&columns=ivint_eod&columns=mean_eod&columns=rho_eod&columns=theta_eod&"
                    "columns=vega_eod&columns=changepercent_eod&columns=change_eod&columns=volume_eod&"
                    "columns=quotetime&columns=rho&columns=strike&columns=style&columns=theta&columns=vega&"
                    "columns=expirationdate&columns=forwardprice&columns=forwardprice_eod&columns=days&"
                    "columns=days_eod&columns=iv&columns=iv_eod&rtMode=RT&userId=OIC_test&uuid=null",
                    headers=await self.get_headers(),
                )
                data = response.json()
                data = OICOptionsMonitor(data)  # Per your original example
                df = data.as_dataframe
                df["ticker"] = symbol

                return data
        except Exception as e:
            print(e)
            return None

    async def options_monitor_for_tickers(self, tickers: List[str]) -> Dict[str, Optional["OICOptionsMonitor"]]:
        """
        Fetch the options monitor data for multiple tickers concurrently.
        Returns a dict: { ticker: OICOptionsMonitor_instance_or_None }.
        """
        results: Dict[str, Optional["OICOptionsMonitor"]] = {}

        # If you want to run them truly concurrently, use asyncio.gather
        # Otherwise, this loop will still be asynchronous but in sequence.
        tasks = []
        for ticker in tickers:
            tasks.append(asyncio.create_task(self.options_monitor(ticker)))

        # Run all tasks concurrently and gather results
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)

        # Match each completed task back to the original ticker
        for ticker, task_result in zip(tickers, completed_tasks):
            if isinstance(task_result, Exception):
                print(f"Error fetching data for {ticker}: {task_result}")
                results[ticker] = None
            else:
                results[ticker] = task_result

        return results




    # async def get_symbol_ids(self):
    #     async with httpx.AsyncClient(headers=await self.get_headers()) as client:
    #         response = await client.get("https://private-authorization.ivolatility.com/lookup/?region=0&matchingType=CONTAINS&sortField=SYMBOL")
    #         data = response.json()

    #         page  = data['page']

    #         symbol = [i.get('symbol', 'N/A') for i in page]
    #         id = [i.get('stockId', 'N/A') for i in page]


    #         data_dict =  {
    #             'symbol': symbol,
    #             'id': id
    #         }

    #         df = pd.DataFrame(data_dict)

    #         df.to_csv('occ_tickers.csv', index=False)



    async def add_ticker(self, symbol):
        try:

            stockid = await self.get_stock_id(symbol)
            payload = {'groupId': 839, 'stockId': stockid}
            async with httpx.AsyncClient() as client:
                response = await client.post("https://private-authorization.ivolatility.com/favorites/addInstrument?userId=OIC_test&uuid=null", headers=await self.get_headers(), data=payload)



                
            data = response.json()
            print(f"Ticker added. {symbol}. {data}")
        except Exception as e:
            print(e)


    async def stock_monitor(self):
        url = f"https://private-authorization.ivolatility.com/favorites/instruments?rtMode=RT&groupId=839&columns=atclose_eod,updatedate,symbol,last,change,changepercent,ivx30,hv20_eod,optvol,optvolcall,optvolput,openinterest_eod,dividend_date,dividend_amount,dividend_frequency,yield,bid,ask,bid_ask_ratio,mid,bid_size,ask_size,stock_vol,ivx7,ivx14,ivx21,ivx60,ivx90,ivx120,ivx150,ivx180,ivx270,ivx360,ivx720,ivx1080,ivx7_chg,ivx14_chg,ivx21_chg,ivx60_chg,ivx90_chg,ivx120_chg,ivx150_chg,ivx180_chg,ivx270_chg,ivx360_chg,ivx720_chg,ivx1080_chg,ivx7_chg_percent,ivx14_chg_percent,ivx21_chg_percent,ivx60_chg_percent,ivx90_chg_percent,ivx120_chg_percent,ivx150_chg_percent,ivx180_chg_percent,ivx270_chg_percent,ivx360_chg_percent,ivx720_chg_percent,ivx1080_chg_percent,ivx7_chg_open,ivx14_chg_open,ivx21_chg_open,ivx60_chg_open,ivx90_chg_open,ivx120_chg_open,ivx150_chg_open,ivx180_chg_open,ivx270_chg_open,ivx360_chg_open,ivx720_chg_open,ivx1080_chg_open,ivx7_chg_percent_open,ivx14_chg_percent_open,ivx21_chg_percent_open,ivx60_chg_percent_open,ivx90_chg_percent_open,ivx120_chg_percent_open,ivx150_chg_percent_open,ivx180_chg_percent_open,ivx270_chg_percent_open,ivx360_chg_percent_open,ivx720_chg_percent_open,ivx1080_chg_percent_open,high,low,open,price,prev_close,open_interest,high_price_52wk,low_price_52wk,call_vol,put_vol,beta_ratio,corr10d,corr20d,corr30d,corr60d,corr90d,corr120d,corr150d,corr180d,outstanding_shares,market_cap,market_cap_change,industry,sentiment,volatile_rank,volatility_ratio,volatility_change_pct,volatility_per_trade,volatility_per_share,iv_rank,avg_opt_vol_1mo,avg_opt_oi_1mo,price_change,price_change_pct,average_true_range,iv_ratio,earnings_yield,dividend_payout_ratio&userId=OIC_test&uuid=null"


        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=await self.get_headers())

            data = data.json()


        df = pd.DataFrame(data)
        df.to_csv('test.csv')
        return df


    async def stock_info(self, symbol: str):
        try:
            # Get the stock ID from the ticker symbol
            stockId = self.occ_ticker_df.get(symbol)

            print(f"Fetching stock info for {symbol} with stockId={stockId}")

            # Use persistent AsyncClient to reduce overhead
            if not hasattr(self, "http_client") or self.http_client is None:
                self.http_client = httpx.AsyncClient()

            # Make the API request
            response = await self.http_client.get(
                f"https://private-authorization.ivolatility.com/lookup/stockInformation",
                params={
                    "stockId": stockId,
                    "region": 1,
                    "rtMode": "RT",
                    "hasOptions": "true"
                },
                headers=await self.get_headers()
            )

            # Raise an exception for HTTP errors
            response.raise_for_status()

            # Parse the response
            data = response.json()

            # Return the parsed data wrapped in StockInfo
            return StockInfo(data, ticker=symbol)

        except httpx.HTTPStatusError as e:
            # Log HTTP-specific errors for better debugging
            print(f"HTTP error for symbol '{symbol}': {e.response.status_code} - {e.response.text}")
        except Exception as e:
            # Log general exceptions
            print(f"Error fetching stock info for symbol '{symbol}': {e}")
        except asyncio.TimeoutError:
            print(f"Timeout fetching data for {symbol}")
 



    async def vola_snapshot(self, symbol):
        """
        Fetches and returns stock volatility information for the given symbol.

        >>> RETURNS FOUR ITEMS.. YOU MUST CALL IT AS..

        a,b,c,d = await occ.vola_snapshot()
        """
        url = f"https://private-authorization.ivolatility.com/widget/stock-vola-info?symbol={symbol}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=await self.get_headers())
            data = data.json()

            vola_snapshot_day = data.get('volaSnapshotDay')
            vola_snapshot_week = data.get('volaSnapshotWeek')
            vola_snapshot_month = data.get('volaSnapshotMonth')
            all_compound_vola = data.get('allCompBoundVola')    

            vola_day = VolaSnapshot(vola_snapshot_day, symbol)
            vola_week = VolaSnapshot(vola_snapshot_week,symbol)
            vola_month = VolaSnapshot(vola_snapshot_month,symbol)
            all_compound = AllCompBoundVola(all_compound_vola,symbol)


            return vola_day, vola_week, vola_month, all_compound


    async def historic_ivx(self, symbol):
        url = f"https://private-authorization.ivolatility.com/widget/ivx-hv?symbol={symbol}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=await self.get_headers())
            data = data.json()

            return HistoricIVX(data, symbol)

    async def volatility_scale(self, symbol):
        url = f"https://private-authorization.ivolatility.com/widget/volatility-scale?symbol={symbol}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=await self.get_headers())

            data = data.json()

            return VolatilityScale(data, symbol)

    async def correlation_beta(self, symbol):
        """RETURNS A TUPLE"""
        url = f"https://private-authorization.ivolatility.com/widget/correlation-beta/today?symbol={symbol}"
        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=await self.get_headers())
            data = data.json()
            beta = round(float(data.get('beta', 0))*100,3)
            correlation = round(float(data.get('correlation'))*100,3)

            return beta, correlation


    async def expiry_dates(self, symbol:str='AAPL'):
        headers = await self.get_headers()
        id = await self.get_stock_id(symbol)
        endpoint = f"https://private-authorization.ivolatility.com/optcalc/dictionary/expiration/{id}?isEod=false"
        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.get(endpoint)
            data = data.json()


            return ExpiryDates(data)
        

    async def get_prob(self, ticker, expiry, days='10', type='ATM', term='0', is_eod='false', db=None):
        """
        Main function to fetch and calculate option probabilities
        """
        async def calculate_and_insert_probabilities(get_probability, ticker, expiry, price, volatility, db, table_name):
            try:
                # Create dataframe from fetched probability data
                df = pd.DataFrame(get_probability, index=[0])
                
                # Add ticker, expiry, and current price to the dataframe
                df['ticker'] = ticker
                df['expiry'] = expiry
                df['current_price'] = price
                
                # Extract standard deviation columns
                std1 = df['std1'].values[0]
                negative_std1 = df['negativeStd1'].values[0]  # Assuming negativeStd1 column exists
                
                # Calculate days to expiry
                expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
                current_date = datetime.now()
                days_to_expiry = (expiry_date - current_date).days

                # Calculate sigma dynamically based on volatility and days to expiry
                sigma = self.calculate_sigma(price, volatility, days_to_expiry)

                # Calculate Z-scores for std1 and negativeStd1
                z_score_std1 = (std1 - price) / sigma if sigma != 0 else 0
                z_score_negative_std1 = (negative_std1 - price) / sigma if sigma != 0 else 0

                # Calculate probabilities for std1 and negativeStd1
                df['prob_below_std1'] = norm.cdf(z_score_std1)
                df['prob_above_std1'] = 1 - df['prob_below_std1']

                df['prob_below_negative_std1'] = norm.cdf(z_score_negative_std1)
                df['prob_above_negative_std1'] = 1 - df['prob_below_negative_std1']
        
                # Debugging: print the probabilities
                print(f"Prob below std1: {df['prob_below_std1'].values[0]}, Prob above std1: {df['prob_above_std1'].values[0]}")
                print(f"Prob below negativeStd1: {df['prob_below_negative_std1'].values[0]}, Prob above negativeStd1: {df['prob_above_negative_std1'].values[0]}")
                
                # Insert the updated dataframe into the database
                await db.batch_upsert_dataframe(df, table_name=table_name, unique_columns=['ticker', 'expiry'])

                return df
            except Exception as e:
                print(f"Error inserting data for {ticker}: {e}")

        try:
            stock_id = await self.get_stock_id(ticker)

            if type == 'ATM':
                term = '0'

            # Fetch volatility data
            url=f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/calcVola?stockid={stock_id}&expDate={expiry}&days={days}&typeVola={type}&term={term}&isEod={is_eod}"
            headers=await self.get_headers()
            async with httpx.AsyncClient() as client:
                data = await client.get(url, headers=headers)
                if data.status_code == 200:
                    vola = data.json()
                    vola = vola.get('vola')
                    expiry_str = datetime.strptime(expiry, "%Y-%m-%d")

                    # Calculate the difference in time
                    # Get stock info
                    stock_info = await self.stock_info(ticker)
                    price = stock_info.price
                    div_yield = stock_info.dividendYield
                    rate = await self.get_rate(stock_id)

                    formatted_time = expiry_str.strftime("%H:%M")
                    url = f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/calculate?price={price}&expDate={expiry}%20{formatted_time}&daysToExp={rate.daysToExp}&hoursToExp={rate.hours}&minutesToExp={rate.minutes}&volaType={type}&vola={vola}&rate={rate.rate}&lastDividentDate={rate.lastDividendDate}&lastDividentAmount={rate.lastDividendAmount}&dividentFrequency={rate.dividendFrequency}&yield={div_yield}&stockId={stock_id}&dividendType=Regular&isEod=false"
                    print(url)
                    
                    # Fetch probability data
                    async with httpx.AsyncClient() as client:
                        data = await client.get(url, headers=headers)
                        if data.status_code == 200:
                            data = data.json()
                            df = pd.DataFrame(data, index=[0])
                            # Call the function to calculate and insert the probabilities
                            await calculate_and_insert_probabilities(data, ticker, expiry, price,volatility=vola, db=db, table_name='probabilities')
                            return df
        except Exception as e:
            print(f"Error processing {ticker}: {e}")


    async def get_price(self, ticker):
        try:
            if ticker in ['SPX', 'NDX', 'XSP', 'RUT', 'VIX']:
                ticker = f"I:{ticker}"
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=1&apiKey={self.api_key}"
            print(url)
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
            print(e)

    async def get_probability(self, ticker, vola_type='ATM', term:int=30):
        try:
            stock_id = await self.get_stock_id(ticker)
            async with httpx.AsyncClient() as client:
                data = await client.get(f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/parameters?stockid={stock_id}&isEod=false", headers=await self.get_headers())

                if data.status_code == 200:

                    data = data.json()
                    style = data.get('style')
                    price = data.get('price')
                    priceStrike = data.get('priceStrike')
                    expDate = data.get('expDate')
                    expDate = expDate[:10]  # Get '2024-10-18'
                    daysToExp = data.get('daysToExp')
                    hours = data.get('hours')
                    minutes = data.get('minutes')
                    rate = data.get('rate')
                    lastDividendDate = data.get('lastDividendDate')
                    lastDividendDate = lastDividendDate[:10]
                    lastDividendAmount = data.get('lastDividendAmount')
                    dividendFrequency = data.get('dividendFrequency')
                    regionTitle = data.get('regionTitle')
                    regionId = data.get('regionId')
                    stockType = data.get('stockType')

                    vola_type = vola_type
                    if vola_type == 'ATM':
                        term = 0
                    
                    async with httpx.AsyncClient() as client:
                        vola_url = await client.get(f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/calcVola?stockid={stock_id}&expDate={expDate}&days={daysToExp}&typeVola={vola_type}&term={term}&isEod=false", headers=await self.get_headers())

                        if vola_url.status_code == 200:
                            vola_url = vola_url.json()

                            vola = vola_url.get('vola')
                            print(vola)

                    async with httpx.AsyncClient() as client:
                        price = await self.get_price(ticker)
                        # Dynamically calculate P1 (1% below current price) and P2 (1% above current price)
                        p1 = price * 0.99  # 1% below the current price
                        p2 = price * 1.01  # 1% above the current price
                        data = await client.get(f"https://private-authorization.ivolatility.com/optcalc/probability-calculator/calculate?price={price}&expDate={expDate}%2000:00&daysToExp={daysToExp}&hoursToExp={hours}&minutesToExp={minutes}&volaType={vola_type}&vola={vola}&rate={rate}&lastDividentDate={lastDividendDate}&lastDividentAmount={lastDividendAmount}&dividentFrequency={dividendFrequency}&yield=0.00&stockId={stock_id}&p1={p1}&p2={p2}&dividendType=Regular&isEod=false", headers=await self.get_headers())

                        if data.status_code == 200:
                            print(data)
                            data = data.json()

            
                            between = round(float(data.get('between'))*100,2)
                            below = round(float(data.get('below'))*100,2)
                            above = round(float(data.get('above'))*100,2)
                            negativeStd1 = round(data.get('negativeStd1'),2)
                            negativeStd2 = round(data.get('negativeStd2'),2)
                            negativeStd3 = round(data.get('negativeStd3'),2)
                            std1 = round(data.get('std1'),2)
                            std2 = round(data.get('std2'),2)
                            std3 = round(data.get('std3'),2)
                            both = round(float(data.get('both'))*100,2)
                            either = round(float(data.get('either'))*100,2)
                            neither = round(float(data.get('neither'))*100,2)


                            dict = { 
                                'ticker': ticker,
                                'expiry': expDate,
                                'vola': vola,
                                'vola_type': vola_type,
                                'term': term,
                                'low_price': round(p1,2),
                                'high_price': round(p2,2),
                                'between': between,
                                'below': below,
                                'above': above,
                                'both': both,
                                'either': either,
                                'neither': neither,
                                'negstd1': negativeStd1,
                                'negstd2': negativeStd2,
                                'negstd3': negativeStd3,
                                'price': price,
                                'std1': std1,
                                'std2': std2,
                                'std3': std3,
                            }


                            df = pd.DataFrame(dict, index=[0])

                        
                            return df
        except Exception as e:
            print(e)



    async def get_stock_info_for_tickers(self, tickers: list):
        """
        Fetch stock information for multiple tickers concurrently.
        Returns a dictionary {ticker: StockInfo}.
        """
        results = {}
        
        # Ensure a shared AsyncClient exists
        if not hasattr(self, "http_client") or self.http_client is None:
            self.http_client = httpx.AsyncClient()

        async def fetch_stock_info(ticker):
            """
            Helper function to fetch stock info for a single ticker.
            """
            try:
                # Get stock info using the stock_info method
                return await self.stock_info(ticker)
            except Exception as e:
                print(f"Error fetching stock info for ticker '{ticker}': {e}")
                return None

        # Prepare all tasks
        tasks = [fetch_stock_info(ticker) for ticker in tickers]

        # Run tasks in parallel
        fetched_data = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results into a dictionary keyed by ticker
        for ticker, stock_info in zip(tickers, fetched_data):
            if isinstance(stock_info, Exception):
                print(f"Failed to fetch stock info for {ticker}: {stock_info}")
                results[ticker] = None
            else:
                results[ticker] = stock_info

        return results
    

    def sanitize_data(self,value):
        """Convert 'Infinity', 'inf', '-Infinity', '-inf', and 'NaN' values to NULL or a large finite number."""
        if isinstance(value, str) and value.lower() in ('infinity', '-infinity', 'inf', '-inf', 'nan'):
            return None  # Convert problematic values to NULL for PostgreSQL
        return value


    async def new_occ(self):
        """ASYNC GENERATOR - YIELDS DATAFRAME"""
        x = await self.get_headers()


        async with aiohttp.ClientSession() as session:
            async with session.get("https://private-authorization.ivolatility.com/favorites/instruments?rtMode=RT&&groupId=14&columns=highprice52wk_eod,lowprice52wk_eod,dividenddate_eod,dividendamount_eod,dividendfrequency_eod,dividendyield_eod,optvol_eod,optvol,optvolcall_eod,optvolcall,optvolput_eod,optvolput,openinterest_eod,openinterest,openinterestcall_eod,openinterestput_eod,stockvolume_eod,stockvolume,avgoptvol1wk_eod,avgoptoi1wk_eod,avgoptvol1mo_eod,avgoptoi1mo_eod,callputvolumeratio,callputvolumeratio_eod,putcallvolumeratio,putcallvolumeratio_eod,hv10_eod,hv20_eod,hv20,hv30_eod,hv60_eod,hv90_eod,hv120_eod,hv150_eod,hv180_eod,hv30hl_eod,hv60hl_eod,hv90hl_eod,hv120hl_eod,hv150hl_eod,hv180hl_eod,ivx30hv20_eod,ivx30hv20,correlation10d_eod,correlation20d_eod,correlation30d_eod,correlation60d_eod,correlation90d_eod,correlation120d_eod,correlation150d_eod,correlation180d_eod,beta10d_eod,beta20d_eod,beta30d_eod,beta60d_eod,beta90d_eod,beta120d_eod,beta150d_eod,beta180d_eod,bidsize,asksize,outstandingshares_eod,marketcap_eod,eps_eod,pe_eod,industry,cumulativeprice_eod,cumulativevalue_eod,pcbsize_eod,pcasize_eod,vwap_eod,vwap,ivp30_eod,ivp30,ivp60_eod,ivp90_eod,ivp120_eod,ivp150_eod,ivp180_eod,ivr30_eod,ivr30,ivr60_eod,ivr90_eod,ivr120_eod,ivr150_eod,ivr180_eod,highdate52wk_eod,lowdate52wk_eod,hvp10_eod,hvp20_eod,hvp30_eod,hvp60_eod,hvp90_eod,hvp120_eod,hvp150_eod,hvp180_eod,open,high,low,callputivxratio,d10_eod,d10,d25_eod,d25&userId=OIC_test&uuid=null",headers=x) as resp:

                data = await resp.json()

                data = NEWOIC(data)
                data.as_dataframe = data.as_dataframe.rename(columns={'symbol': 'ticker'})
                
                return data
            


    async def fetch_options_monitor_columns(self):
        url = (
            "https://private-authorization.ivolatility.com/iv-live/assets/"
            "option_monitor_columns.json?version=5&userId=OIC_test&uuid=null"
        )
        headers = await self.get_headers()
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
        
        # Extract the gridColumns dictionary
        grid_columns = data.get("gridColumns", {})
        
        # Build a new dictionary with each column's key and the key properties.
        columns_info = {}
        for col_key, col_props in grid_columns.items():
            columns_info[col_key] = {
                "dataField": col_props.get("dataField"),
                # If a "description" is provided use it; otherwise, fallback to "caption"
                "description": col_props.get("description") or col_props.get("caption"),
                "template": col_props.get("template")
            }
        return columns_info
    async def save_columns_info_to_file(self, filename="options_monitor_columns.json"):
        columns_info = await self.fetch_options_monitor_columns()
        # Save the resulting dictionary to a JSON file with indent for readability.
        with open(filename, "w") as f:
            json.dump(columns_info, f, indent=2)
        print(f"Columns info saved to {filename}")
    async def fetch_favourites_columns(self):
        url = "https://private-authorization.ivolatility.com/iv-live/assets/favourites_columns.json?version=5&userId=OIC_test&uuid=null"
        headers = await self.get_headers()
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
        
        # Extract the gridColumns dictionary from the data.
        grid_columns = data.get("gridColumns", {})
        
        # Build a new dictionary that uses each key (column name) and extracts the key properties.
        columns_info = {}
        for col_key, col_props in grid_columns.items():
            columns_info[col_key] = {
                "dataField": col_props.get("dataField"),
                # Use "description" if available, otherwise fall back to "caption"
                "description": col_props.get("description") or col_props.get("caption"),
                "template": col_props.get("template")
            }
        return columns_info

    async def save_favourites_columns_to_file(self, filename="stock_monitor_columns.json"):
        columns_info = await self.fetch_favourites_columns()
        with open(filename, "w") as f:
            json.dump(columns_info, f, indent=2)
        print(f"Columns info saved to {filename}")



    async def get_vola(self, ticker):
        id = await self.get_stock_id(ticker)
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://private-authorization.ivolatility.com/optcalc/option-calculator/parameters?stockid={id}&isEod=false", headers=await self.get_headers()) as resp:

                data = await resp.json()


                return CalculatorMetrics(data)
            


    async def calculate_iv_and_greeks(self, ticker, style):

        stock_id = await self.get_stock_id(ticker)


        calc_info = await self.get_vola(ticker)

        expiry = calc_info.expiry.split('T')[0]
        minutes = calc_info.minutes
        if minutes < 10:
            minutes = f"0{minutes}"


        expDate = f"{expiry} {calc_info.hours}:{minutes}"
        price = calc_info.price
        strike = calc_info.strike
        daysToExp = calc_info.dte
        hoursToExp = calc_info.hours
        minutesToExp = calc_info.minutes
        vola = calc_info.vola
        rate = calc_info.rate
        last_div_date = calc_info.last_div_date.split('T')[0].replace('-','')
        last_div_amt = calc_info.last_div_amount
        div_freq = calc_info.div_freq




        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://private-authorization.ivolatility.com/optcalc/option-calculator/calculate?style={style}&price={price}&priceStrike={strike}&{expDate}&daysToExp={daysToExp}&hoursToExp={hoursToExp}&minutesToExp={minutesToExp}&vola={vola}&rate={rate}&lastDividentDate={last_div_date}&lastDividentAmount={last_div_amt}&dividentFrequency={div_freq}&yield=0.00&stockId={stock_id}&dividendType=Regular&number=2&isEod=false", headers=await self.get_headers()) as resp:
                data = await resp.json()


                return CalculateGreeks(data)



    def flex_report_batch(self, report_date: str) -> pd.DataFrame:
        """
        Downloads the OCC Open Interest (OI) report for both equity and index tickers and combines them.
        
        The OCC URL uses:
        - reportType=OI 
        - optionType: "E" for equity tickers and "I" for index tickers.
        - reportDate: in YYYYMMDD format.
        
        The expected line format in the OI report is (example):
            1AAL     C   03 06 2025  00019 530      0.1046     13790
            
        Fields captured:
        Symbol, OptionType, Month, Day, Year, Strike_raw, Mark, OpenInterest

        Processing steps:
        1. Remove leading digits from Symbol to form a clean ticker.
        2. Map OptionType "C" and "P" to "call" and "put" (stored in column "call_put").
        3. Combine Month, Day, and Year into an "expiry" column (formatted as YYYY-MM-DD).
        4. Process Strike_raw by removing whitespace and dividing by 1000 to get the strike.
        5. Convert Mark and OpenInterest to numeric.
        6. Add a "marketType" column ("equity" for optionType "E", "index" for "I").
        7. Combine the two reports into one DataFrame.
        
        Parameters:
        report_date: A string representing the report date in YYYYMMDD format (e.g., "20190531")
        
        Returns:
        A pandas DataFrame containing open interest data for both equity and index tickers.
        """
        # Mapping for the two market types.
        market_types = {"E": "equity", "I": "index"}
        df_list = []

        # Loop over both market types.
        for opt in market_types:
            url = f"https://marketdata.theocc.com/flex-reports?reportType=OI&optionType={opt}&reportDate={report_date}"
            print(url)
            # Download the file.
            response = requests.get(url)
            response.raise_for_status()
            
            # Write content to a temporary file.
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=".txt") as tmp_file:
                temp_filename = tmp_file.name
                tmp_file.write(response.content)
            
            # Define the regex pattern.
            # This pattern matches lines such as:
            #    1AAL     C   03 06 2025  00019 530      0.1046     13790
            # Capturing:
            #    Symbol, OptionType, Month, Day, Year, Strike_raw, Mark, OpenInterest
            pattern = re.compile(
                r'^\s*(\S+)\s+([CP])\s+(\d{2})\s+(\d{2})\s+(\d{4})\s+(\d{5}\s+\d{3})\s+([\d\.]+)\s+(\d+)',
                re.MULTILINE
            )
            
            matches = []
            with open(temp_filename, 'r') as f:
                for line in f:
                    line = line.rstrip('\n')
                    m = pattern.match(line)
                    if m:
                        matches.append(m.groups())
            
            # Delete the temporary file.
            os.remove(temp_filename)
            
            # Create a DataFrame from the matches.
            df_temp = pd.DataFrame(matches, columns=[
                "Symbol", "OptionType", "Month", "Day", "Year", "Strike_raw", "Mark", "OpenInterest"
            ])
            
            # Convert date parts to numeric.
            df_temp["Month"] = pd.to_numeric(df_temp["Month"], errors='coerce')
            df_temp["Day"]   = pd.to_numeric(df_temp["Day"], errors='coerce')
            df_temp["Year"]  = pd.to_numeric(df_temp["Year"], errors='coerce')
            # Create expiry column in YYYY-MM-DD format.
            df_temp["expiry"] = pd.to_datetime(df_temp[['Year', 'Month', 'Day']]).dt.strftime('%Y-%m-%d')
            
            # Remove leading digits from Symbol.
            df_temp["ticker"] = df_temp["Symbol"].str.replace(r'^\d+', '', regex=True)
            
            # Map OptionType "C"/"P" to "call"/"put".
            df_temp["call_put"] = df_temp["OptionType"].map({'C': 'call', 'P': 'put'})
            
            # Process the strike field:
            # Remove all whitespace from Strike_raw, convert to int, then divide by 1000.
            df_temp["strike"] = df_temp["Strike_raw"].str.replace(r'\s+', '', regex=True).astype(int) / 1000.0
            
            # Convert Mark and OpenInterest to numeric.
            df_temp["mark"] = pd.to_numeric(df_temp["Mark"], errors='coerce')
            df_temp["OpenInterest"] = pd.to_numeric(df_temp["OpenInterest"], errors='coerce')
            
            # Add marketType column.
            df_temp["marketType"] = market_types[opt]
            
            # Drop unnecessary columns.
            df_temp = df_temp.drop(columns=["Year", "Month", "Day", "Symbol", "OptionType", "Strike_raw", "Mark"])
            
            # Reorder columns (optional).
            df_temp = df_temp[["ticker", "call_put", "expiry", "strike", "mark", "OpenInterest", "marketType"]]
            df_temp = df_temp.rename(columns={'OpenInterest': 'oi', 'marketType': 'type'})
            # Append to our list.
            df_list.append(df_temp)
        
        # Combine both DataFrames.
        df_final = pd.concat(df_list, ignore_index=True)
        return df_final
    def daily_oi_csv(self, date: str) -> pd.DataFrame:
        """
        Downloads the OCC daily open interest CSV for the given report date,
        and returns a DataFrame with a flattened header.
        
        1) Skips the first row ("Daily Open Interest - February 2025")
        2) Takes the second row (row index 1) and the third row (row index 2)
           as the two header rows.
        3) Moves the "Date" cell and the "OCC Total" cell from the second row
           down to the third row, so the third row actually holds "Date" in the
           first column and "OCC Total" in the last column.
        4) Fills forward the rest of row2 for multi-column group labels
           (e.g. "Equity", "Index/Other", "Debt", "Futures").
        5) Combines row2 & row3 columnwise (joining non-empty strings with a space).
        6) The remaining lines (from row 4 onward) become the data portion.
        
        Example structure in the CSV:
        
          Row 1:  Daily Open Interest - February 2025, ...
          Row 2:  Date, Equity, , , Index/Other, , , Debt, , , Futures, OCC Total
          Row 3:  , Calls, Puts, Total, Calls, Puts, Total, Calls, Puts, Total, Total, 
          Row 4+: 02/07/2025, "236,387,416", "190,001,849", ...
        
        Returns:
            pd.DataFrame with a cleaned header and the data rows.
        """
        # Convert date from YYYY-MM-DD to MM/DD/YYYY
        converted_date = pd.to_datetime(date, format="%Y-%m-%d").strftime("%m/%d/%Y")
        url = f"https://marketdata.theocc.com/daily-open-interest?reportDate={converted_date}&action=download&format=csv"
        
        # Download the CSV
        response = requests.get(url)
        response.raise_for_status()
        csv_text = response.content.decode('utf-8', errors='replace')
        
        # Split lines, ignoring empties
        lines = [line for line in csv_text.splitlines() if line.strip()]
        
        if len(lines) < 4:
            raise ValueError("Not enough lines in CSV to form the required header and data.")
        
        # Skip the first line (title row). Then row2 = lines[1], row3 = lines[2].
        row2 = lines[1].split(',')
        row3 = lines[2].split(',')
        
        # Ensure row2 and row3 have the same length by extending with blanks if needed
        ncols = max(len(row2), len(row3))
        while len(row2) < ncols:
            row2.append("")
        while len(row3) < ncols:
            row3.append("")
        
        # Move "Date" from row2[0] down into row3[0]
        # Move "OCC Total" from row2[-1] down into row3[-1]
        # Then blank out row2[0] and row2[-1] so that they can be merged with row3's cells.
        if row2[0]:
            row3[0] = row2[0]
            row2[0] = ""
        if row2[-1]:
            row3[-1] = row2[-1]
            row2[-1] = ""
        
        # Fill forward row2 so that multi-column group labels appear.
        for i in range(ncols):
            if i > 0 and row2[i] == "":
                row2[i] = row2[i-1]
        
        # Combine row2 & row3 to form final_header
        final_header = []
        for i in range(ncols):
            top = row2[i].strip()
            bot = row3[i].strip()
            if top and bot:
                col = f"{top} {bot}"
            else:
                col = top or bot
            final_header.append(col)
        
        # Make the final_header unique
        final_header = make_unique(final_header)
        
        # Remaining lines are data (from line index 3 onward)
        data_lines = lines[3:]
        data_str = "\n".join(data_lines)
        
        # Read data with the combined header
        from io import StringIO
        data_io = StringIO(data_str)
        df = pd.read_csv(data_io, header=None, names=final_header, engine='python')
        df = df.rename(columns={'Date': 'date', 'Equity Calls': 'equity_calls', 'Equity Puts': 'equity_puts', 'Equity Total': 'equity_total', 'Index/Other Calls': 'index_other_calls', 'Debt Calls': 'debt_calls', 'Debt Puts': 'debt_puts', 'Debt Total': 'debt_total', 'Futures Total': 'futures_total', 'Futures OCC Total': 'future_occ_total', 'Index/Other Puts': 'index_other_puts', 'Index/Other Total': 'index_other_total'})
        print(df.columns)
        return df
    


    def occ_position_limits_batch(self, report_date: str) -> pd.DataFrame:
        """
        Downloads and parses the 'position limits' change report for a given date.
        
        The 'report_date' should be in 'YYYY-MM-DD' format. Example: '2025-07-18'.
        The downloaded file has columns like:
            Equity_Symbol, ,Start_Date,Start_Pos_Limit,End_Date,End_Pos_Limit,Action
        We remove the extra blank column and return a clean DataFrame.
        """
        # 1) Build the URL
        url = f"https://marketdata.theocc.com/position-limits?reportType=change&reportDate={report_date}"

        # 2) Download the file
        response = requests.get(url)
        response.raise_for_status()

        # 3) Write the content to a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=".txt") as tmp_file:
            temp_filename = tmp_file.name
            tmp_file.write(response.content)

        try:
            # 4) Parse the CSV-like file
            #    The first row is a header with 7 columns, but the 2nd column is blank.
            df = pd.read_csv(
                temp_filename,
                sep=',',         # comma-delimited
                engine='python', # often safer for “odd” CSVs
                header=0,        # first row is the header
                skip_blank_lines=True
            )
        finally:
            # 5) Clean up the temporary file
            os.remove(temp_filename)

        # 6) Drop the extra blank column, often labeled "Unnamed: 1"
        #    (Check that the second column is actually blank or unnamed before dropping)
        if len(df.columns) > 1 and "Unnamed" in df.columns[1]:
            df.drop(columns=[df.columns[1]], inplace=True)

        # 7) Return the cleaned DataFrame
        df = df.rename(columns={'Equity Symbol': 'symbol', 'Start_Date': 'start_date', 'Start_Pos_limit': 'start_pos_limit', 'End_Date': 'end_date', 'End_Pos_Limit': 'end_pos_limit', 'Action': 'action'})
        return df
    


    def occ_threshold_batch(self, date: str) -> pd.DataFrame:
        """
        Downloads and parses the OCC threshold securities report for the specified date.
        
        The user provides a date string in 'YYYY-MM-DD' format (for example, '2025-02-07').
        This function converts it to 'YYYYMMDD' (e.g. '20250207') for the URL, downloads
        the file, saves it to a temporary file, reads it as pipe-delimited, deletes
        the temp file, and returns a pandas DataFrame.
        """
        # Convert from 'YYYY-MM-DD' to 'YYYYMMDD'
        date_str = pd.to_datetime(date, format="%Y-%m-%d").strftime("%Y%m%d")
        
        # Construct the URL
        url = f"https://marketdata.theocc.com/threshold-securities?reportDate={date_str}"
        
        # Download the file
        response = requests.get(url)
        response.raise_for_status()
        
        # Write to a temporary file
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=".txt") as tmp_file:
            temp_filename = tmp_file.name
            tmp_file.write(response.content)
        
        try:
            # Read as pipe-delimited
            df = pd.read_csv(
                temp_filename,
                sep='|',
                engine='python',    # more tolerant of irregularities
                header=0,           # assume first line is the column header
                skip_blank_lines=True
            )
        finally:
            # Always remove the temporary file
            os.remove(temp_filename)
        df = df.rename(columns={'Symbol': 'ticker', 'Security Name': 'security_name', 'Market Category': 'market_category', 'Reg SHO Threshold Flag': 'reg_sho_flag', 'Filler': 'filler', 'Filler.1': 'filler_1'})
        return df
    

    def occ_volume_query_batch(self, date: str, symbol: str, account_type:str='M') -> pd.DataFrame:
        """
        Fetches the OCC volume query CSV for a given date (YYYY-MM-DD) and symbol (e.g. 'IBM'),
        saves it to a temporary file, reads it into a DataFrame, and returns the DataFrame.
        
        The CSV columns include:
        quantity, underlying, symbol, actype, porc, exchange, actdate, contractDate
        
        Example usage:
        df = occ_volume_query("2025-02-07", "IBM")
        """
        # Convert the user-supplied date (YYYY-MM-DD) to the format YYYYMMDD.
        date_str = pd.to_datetime(date, format="%Y-%m-%d").strftime("%Y%m%d")
        
        # Build the URL with the given date_str and symbol.
        url = (
            f"https://marketdata.theocc.com/volume-query?"
            f"reportDate={date_str}&format=csv&volumeQueryType=O&symbolType=O"
            f"&symbol={symbol.upper()}&reportType=D&accountType={account_type}"
            f"&productKind=ALL&porc=BOTH&contractDt={date_str}"
        )
        
        # Download the CSV file.
        response = requests.get(url)
        response.raise_for_status()
        
        # Write the content to a temporary file.
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=".csv") as tmp_file:
            temp_filename = tmp_file.name
            tmp_file.write(response.content)
        
        try:
            # Read the CSV into a DataFrame.
            df = pd.read_csv(temp_filename, engine='python')
        finally:
            # Delete the temporary file.
            os.remove(temp_filename)
        
        return df