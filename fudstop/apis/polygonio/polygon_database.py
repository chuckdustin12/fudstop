
from .async_polygon_sdk import Polygon
from .polygon_options import PolygonOptions
import pandas as pd
from tabulate import tabulate
import asyncio
from .models.option_models.universal_snapshot import SpxSnapshot
import os
from datetime import datetime, timedelta
import aiohttp
from aiohttp.client_exceptions import ContentTypeError
from dotenv import load_dotenv
load_dotenv()


class PolygonDatabase(PolygonOptions, Polygon):
    def __init__(self, host='localhost', user='chuck', database='market_data', password='fud', port=5432, connection_string:str=None):
        self.most_active_tickers= ['SNOW', 'IBM', 'DKNG', 'SLV', 'NWL', 'SPXS', 'DIA', 'QCOM', 'CMG', 'WYNN', 'PENN', 'HLF', 'CCJ', 'WW', 'NEM', 'MOS', 'SRPT', 'MS', 'DPST', 'AG', 'PAA', 'PANW', 'XPEV', 'BHC', 'KSS', 'XLP', 'LLY', 'MDB', 'AZN', 'NVO', 'BOIL', 'ZM', 'HUT', 'VIX', 'PDD', 'SLB', 'PCG', 'DIS', 'TFC', 'SIRI', 'TDOC', 'CRSP', 'BSX', 'BITF', 'AAL', 'EOSE', 'RIVN', 'X', 'CCL', 'SOXS', 'NOVA', 'TMUS', 'HES', 'LI', 'NVAX', 'TSM', 'CNC', 'IAU', 'GDDY', 'CVX', 'TGT', 'MCD', 'GDXJ', 'AAPL', 'NKLA', 'EDR', 'NOK', 'SPWR', 'NKE', 'HYG', 'FSLR', 'SGEN', 'DNN', 'BAX', 'CRWD', 'OSTK', 'XLC', 'RIG', 'SEDG', 'SNDL', 'RSP', 'M', 'CD', 'UNG', 'LQD', 'TTD', 'AMGN', 'EQT', 'YINN', 'MULN', 'FTNT', 'WBD', 'MRNA', 'PTON', 'SCHW', 'ABNB', 'EW', 'PM', 'UCO', 'TXN', 'DLR', 'KHC', 'MMAT', 'QQQ', 'GOOGL', 'AEM', 'RTX', 'AVGO', 'RBLX', 'PAAS', 'UUP', 'OXY', 'SQ', 'PLUG', 'CLF', 'GOEV', 'BKLN', 'ALB', 'BALL', 'SMH', 'CVE', 'F', 'KRE', 'TWLO', 'ARCC', 'ARM', 'U', 'SOFI', 'SBUX', 'FXI', 'BMY', 'HSBC', 'EFA', 'SVXY', 'VALE', 'GOLD', 'MSFT', 'OIH', 'ARKK', 'AMD', 'AA', 'DXCM', 'ABT', 'WOLF', 'FDX', 'SOXL', 'MA', 'KWEB', 'BP', 'SNAP', 'NLY', 'KGC', 'URA', 'UVIX', 'KMI', 'ACB', 'NET', 'W', 'GRAB', 'LMT', 'EPD', 'FCX', 'STNE', 'NIO', 'SU', 'ET', 'CVS', 'ADBE', 'MXL', 'HOOD', 'FUBO', 'RIOT', 'CRM', 'TNA', 'DISH', 'XBI', 'VFS', 'GPS', 'NVDA', 'MGM', 'MRK', 'ABBV', 'LABU', 'BEKE', 'VRT', 'LVS', 'CPNG', 'BA', 'MTCH', 'PEP', 'EBAY', 'GDX', 'XLV', 'UBER', 'GOOG', 'COF', 'XLU', 'BILI', 'XLK', 'VXX', 'DVN', 'MSOS', 'KOLD', 'XOM', 'BKNG', 'SPY', 'RUT', 'CMCSA', 'STLA', 'NCLH', 'GRPN', 'ZION', 'UAL', 'GM', 'NDX', 'TQQQ', 'COIN', 'WBA', 'CLSK', 'NFLX', 'FREY', 'AFRM', 'NAT', 'EEM', 'IYR', 'KEY', 'OPEN', 'DM', 'TSLA', 'BXMT', 'T', 'TZA', 'BAC', 'MARA', 'UVXY', 'LOW', 'COST', 'HL', 'CHTR', 'TMF', 'ROKU', 'DOCU', 'PSEC', 'XHB', 'VMW', 'SABR', 'USB', 'DDOG', 'DB', 'V', 'NOW', 'XRT', 'SMCI', 'PFE', 'NYCB', 'BIDU', 'C', 'SPX', 'ETSY', 'EMB', 'SQQQ', 'CHPT', 'DASH', 'VZ', 'DNA', 'CL', 'ANET', 'WMT', 'MRO', 'WFC', 'MO', 'USO', 'ENVX', 'INTC', 'GEO', 'VFC', 'WE', 'MET', 'CHWY', 'PBR', 'KO', 'TH', 'QS', 'BTU', 'GLD', 'JD', 'XLY', 'KR', 'ASTS', 'WDC', 'HTZ', 'XLF', 'COP', 'PATH', 'SHEL', 'MXEF', 'SE', 'SPCE', 'UPS', 'RUN', 'DOW', 'ASHR', 'ONON', 'DAL', 'SPXL', 'SAVE', 'LUV', 'HD', 'JNJ', 'LYFT', 'UNH', 'BBY', 'CZR', 'NEE', 'STNG', 'SPXU', 'MMM', 'VNQ', 'IMGN', 'MSTR', 'AXP', 'TMO', 'XPO', 'FEZ', 'ENPH', 'AX', 'NVCR', 'GS', 'MRVL', 'ADM', 'GILD', 'IBB', 'FTCH', 'PARA', 'PINS', 'JBLU', 'SNY', 'BITO', 'PYPL', 'FAS', 'GME', 'LAZR', 'URNM', 'BX', 'MPW', 'UPRO', 'HPQ', 'AMZN', 'SAVA', 'TLT', 'ON', 'CAT', 'VLO', 'AR', 'IDXX', 'SWN', 'META', 'BABA', 'ZS', 'EWZ', 'ORCL', 'XOP', 'TJX', 'XP', 'EL', 'HAL', 'IEF', 'XLI', 'UPST', 'Z', 'TELL', 'LRCX', 'DLTR', 'BYND', 'PACW', 'CVNA', 'GSAT', 'CSCO', 'NU', 'KVUE', 'JPM', 'LCID', 'TLRY', 'AGNC', 'CGC', 'XLE', 'VOD', 'TEVA', 'JETS', 'UEC', 'FSR', 'ZIM', 'ABR', 'IQ', 'AMC', 'ALLY', 'HE', 'OKTA', 'ACN', 'MU', 'FLEX', 'SHOP', 'PLTR', 'CLX', 'LUMN', 'WHR', 'PAGP', 'IWM', 'WPM', 'TTWO', 'AI', 'ALGN', 'SPOT', 'BTG', 'IONQ', 'GE', 'DG', 'AMAT', 'XSP', 'PG', 'LULU', 'DE', 'MDT', 'RCL']
        self.user=user
        self.database = database
        self.pool = None
        self.host = host
        self.conn = None
        self.password = password
        self.port = port
        if connection_string is not None:
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
        self.one_year_from_now = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        self.one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        self.db_config = {
            "host": self.host, # Default to this IP if 'DB_HOST' not found in environment variables
            "port": self.port, # Default to 5432 if 'DB_PORT' not found
            "user": self.user, # Default to 'postgres' if 'DB_USER' not found
            "password": self.password, # Use the password from environment variable or default
            "database": self.database # Database name for the new jawless database
        }
    async def get_rsi(self, ticker, timespan):
        data = await self.rsi(ticker, timespan=timespan)

        if data is not None:
            try:
                df = data.as_dataframe.iloc[[0]]
                # Proceed with using df

                # Handle the absence of .as_dataframe appropriately

            
                status = 'neutral'  # Set default status to 'neutral'
                if df['rsi_value'].iloc[0] <= 30:
                    status = 'oversold'
                elif df['rsi_value'].iloc[0] >= 70:
                    status = 'overbought'
                
                df['status'] = status
                df['timespan'] = timespan
                print(df)
                await self.batch_insert_dataframe(df, table_name='rsi', unique_columns='ticker, timespan')
            except AttributeError as e:
                print(f"An error occurred: {e}")
            
    async def update_all_rsi(self):
        await self.connect()
        timespans = ['minute', 'hour', 'day', 'week', 'month']
        tasks = [self.get_rsi(i, timespan) for i in self.most_active_tickers for timespan in timespans]
        await asyncio.gather(*tasks)


        



    async def atm_options(self, ticker, lower_percent:int=0.90, upper_percent:int=1.10):
        await self.connect()
        option_symbols_list = await self.update_options(ticker, lower_percent=lower_percent, upper_percent=upper_percent)

        # Using $n placeholders for asyncpg
        placeholders = ', '.join([f'${i+1}' for i in range(len(option_symbols_list))])
        query = f"SELECT ticker, strike, call_put, expiry, mid FROM opts WHERE option_symbol IN ({placeholders})"
        query2 = f"SELECT option_symbol FROM opts where option_symbol in ({placeholders})" 
        # Pass option_symbols_list as a parameter
        records = await self.fetch(query, option_symbols_list)
        records2 = await self.fetch(query2, option_symbols_list)
        df = pd.DataFrame(records, columns = ['sym', 'strike', '','exp', 'price'])
        # Extracting the ticker symbols
        ticker_symbols = [record['option_symbol'] for record in records2]


        return df, ticker_symbols
    

    async def strategy_filter_theta(self, sort_column=None):
        await self.connect()

        data = await self.filter_options(theta_min=-0.03, bid_max=1.50, bid_min=0.22, ask_min=0.24, ask_max =1.55)

        if sort_column is None:
            df = pd.DataFrame(data, # Define the column names based on your database schema
columns = [
    'strike', 'expiry', 'dte', 'time_value', 'moneyness', 'liquidity_score', 'call_put',
    'exercise_style', 'option_symbol', 'theta', 'theta_decay_rate', 'delta', 'delta_theta_ratio',
    'gamma', 'gamma_risk', 'vega', 'vega_impact', 'timestamp', 'oi', 'open', 'high', 'low', 
    'close', 'intrinstic_value', 'extrinsic_value', 'leverage_ratio', 'vwap', 'conditions', 
    'price', 'trade_size', 'exchange', 'ask', 'bid', 'spread', 'spread_pct', 'iv', 'bid_size', 
    'ask_size', 'vol', 'mid', 'change_to_breakeven', 'underlying_price', 'ticker', 'return_on_risk', 
    'velocity', 'sensitivity', 'greeks_balance', 'opp', 'insertion_timestamp'
])
            return df
        else:

            df = pd.DataFrame(data).sort_values(sort_column, ascending=False)
            return df
        
    async def execute(self, query, *args):
        async with self.pool.acquire() as conn:
            await conn.execute(query, *args)
    async def fetch(self, query, params=None):
        try:
            async with self.pool.acquire() as conn:
                # Use conn.fetch with query parameters if params are provided
                if params:
                    records = await conn.fetch(query, *params)
                else:
                    records = await conn.fetch(query)
                return records
        except Exception as e:
            print(e)
        

    async def process_ticker(self, ticker):
        x = await self.get_near_the_money_single(ticker)
        snapshot = await self.get_universal_snapshot(x)
        
        df = pd.DataFrame(snapshot)
        if df.empty or 'implied_volatility' not in df.columns or 'underlying_asset.price' not in df.columns or 'details.strike_price' not in df.columns or 'details.expiration_date' not in df.columns:
            return None

        # Convert expiration date to datetime object for proper sorting
        df['details.expiration_date'] = pd.to_datetime(df['details.expiration_date'])

        # Sort first by expiration date, then by implied volatility
        df = df.sort_values(['details.expiration_date', 'implied_volatility'], ascending=[True, True])

        # Select the option with the closest expiration and lowest IV
        skew_row = df.iloc[0]

        iv_skew = skew_row['details.strike_price'] - skew_row['underlying_asset.price']

        if iv_skew >= 3.5:
            return {
                'ticker': ticker,
                'price': skew_row['underlying_asset.price'],
                'iv_skew': iv_skew,
                'strike': skew_row['details.strike_price'],
                'expiration': skew_row['details.expiration_date']  # Converting datetime to string
            }
        else:
            return None



    async def get_universal_snapshot(self, ticker): #✅
        """Fetches the Polygon.io universal snapshot API endpoint"""
        url=f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&limit=250&apiKey={os.environ.get('YOUR_POLYGON_KEY')}"
        print(url)
  
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as resp:
                    data = await resp.json()
                    results = data['results'] if 'results' in data else None
                    if results is not None:
                        return SpxSnapshot(results)
                    else:
                        return None
            except ContentTypeError:
                pass