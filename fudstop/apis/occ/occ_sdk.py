import os
from dotenv import load_dotenv
load_dotenv
import aiohttp
import asyncio
import pandas as pd
import httpx
from scipy.stats import norm
from typing import Optional
from .occ_models import OptionsMonitor, OICOptionsMonitor, VolaSnapshot,AllCompBoundVola, HistoricIVX, VolatilityScale
from .occ_models import OCCMostActive, StockInfo, ExpiryDates, ProbabilityMetrics, ExpiryDates
from .occ_models import StockLoans, VolumeTotals, DailyMarketShare
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore", message="Precision loss occurred in moment calculation due to catastrophic cancellation")
from .occ_models import flatten_json
from fudstop.apis.helpers import format_large_numbers_in_dataframe
import math
from asyncpg import create_pool
webull_id_df = pd.read_csv('files/ticker_csv.csv')
ticker_df = pd.read_csv('files/occ_tickers.csv')
class occSDK:
    def __init__(self, host: str = 'localhost', port: int = 5432, user: str = 'chuck',
                 password: str = 'fud', database: str = 'market_data', webull_id_df=None, ticker_df=None):

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
            stockLoanResults = entity['stockLoanResults']
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



    async def options_monitor(self, symbol):
        try:
            stockId = await self.get_stock_id(symbol)
            center = await self.get_center(symbol)
            async with httpx.AsyncClient() as client:
                response = await client.get(f"https://private-authorization.ivolatility.com/options-monitor/listOptionDataRow?stockId={stockId}&regionId=1&center={center}&columns=alpha&columns=ask&columns=asksize&columns=asktime&columns=bid&columns=bidsize&columns=bidtime&columns=change&columns=changepercent&columns=delta&columns=theoprice&columns=gamma&columns=ivint&columns=ivask&columns=ivbid&columns=mean&columns=openinterest_eod&columns=optionsymbol&columns=volume&columns=paramvolapercent_eod&columns=alpha_eod&columns=ask_eod&columns=bid_eod&columns=delta_eod&columns=theoprice_eod&columns=gamma_eod&columns=ivint_eod&columns=mean_eod&columns=rho_eod&columns=theta_eod&columns=vega_eod&columns=changepercent_eod&columns=change_eod&columns=volume_eod&columns=quotetime&columns=rho&columns=strike&columns=style&columns=theta&columns=vega&columns=expirationdate&columns=forwardprice&columns=forwardprice_eod&columns=days&columns=days_eod&columns=iv&columns=iv_eod&rtMode=RT&userId=9999999&uuid=null", headers=await self.get_headers())

                data = response.json()
                data =  OICOptionsMonitor(data)
                df = data.as_dataframe
                df['ticker'] = symbol

                return data
        except Exception as e:
            print(e)
        




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
        url = f"https://private-authorization.ivolatility.com/favorites/instruments?rtMode=DELAYED&groupId=839&columns=atclose_eod,updatedate,symbol,last,change,changepercent,ivx30,hv20_eod,optvol,optvolcall,optvolput,openinterest_eod,dividend_date,dividend_amount,dividend_frequency,yield,bid,ask,bid_ask_ratio,mid,bid_size,ask_size,stock_vol,ivx7,ivx14,ivx21,ivx60,ivx90,ivx120,ivx150,ivx180,ivx270,ivx360,ivx720,ivx1080,ivx7_chg,ivx14_chg,ivx21_chg,ivx60_chg,ivx90_chg,ivx120_chg,ivx150_chg,ivx180_chg,ivx270_chg,ivx360_chg,ivx720_chg,ivx1080_chg,ivx7_chg_percent,ivx14_chg_percent,ivx21_chg_percent,ivx60_chg_percent,ivx90_chg_percent,ivx120_chg_percent,ivx150_chg_percent,ivx180_chg_percent,ivx270_chg_percent,ivx360_chg_percent,ivx720_chg_percent,ivx1080_chg_percent,ivx7_chg_open,ivx14_chg_open,ivx21_chg_open,ivx60_chg_open,ivx90_chg_open,ivx120_chg_open,ivx150_chg_open,ivx180_chg_open,ivx270_chg_open,ivx360_chg_open,ivx720_chg_open,ivx1080_chg_open,ivx7_chg_percent_open,ivx14_chg_percent_open,ivx21_chg_percent_open,ivx60_chg_percent_open,ivx90_chg_percent_open,ivx120_chg_percent_open,ivx150_chg_percent_open,ivx180_chg_percent_open,ivx270_chg_percent_open,ivx360_chg_percent_open,ivx720_chg_percent_open,ivx1080_chg_percent_open,high,low,open,price,prev_close,open_interest,high_price_52wk,low_price_52wk,call_vol,put_vol,beta_ratio,corr10d,corr20d,corr30d,corr60d,corr90d,corr120d,corr150d,corr180d,outstanding_shares,market_cap,market_cap_change,industry,sentiment,volatile_rank,volatility_ratio,volatility_change_pct,volatility_per_trade,volatility_per_share,iv_rank,avg_opt_vol_1mo,avg_opt_oi_1mo,price_change,price_change_pct,average_true_range,iv_ratio,earnings_yield,dividend_payout_ratio&userId=OIC_test&uuid=null"


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