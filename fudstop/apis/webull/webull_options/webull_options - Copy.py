import os
from dotenv import load_dotenv
import numpy as np
load_dotenv()
import json
import pickle
import uuid
import re
import asyncio
import httpx
import aiohttp
import pandas as pd
from datetime import datetime
import asyncpg
import time
from fudstop.apis.webull.webull_trading import WebullTrading
trading = WebullTrading()
from webull import webull
wb = webull()
from .models.options_data import From_, GroupData, BaseData, OptionData
from fudstop.apis.polygonio.polygon_options import PolygonOptions
from fudstop.apis.polygonio.polygon_database import PolygonDatabase
from fudstop.apis.helpers import human_readable
from typing import List, Dict
from aiohttp.client_exceptions import ContentTypeError
from .helpers import process_candle_data, get_human_readable_string
from asyncio import Semaphore
from datetime import timedelta
sema = Semaphore(10)

class WebullOptions:
    def __init__(self, database:str='market_data', user:str='chuck'):
        self.db = PolygonDatabase()
        self.database = database
        self.conn = None
        self.pool = None
        self.user=user
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
        self.opts = PolygonOptions(host='localhost', user='chuck', database='market_data', password='fud', port=5432)
        self.most_active_tickers = ['SPY', 'QQQ', 'SPX', 'TSLA', 'AMZN', 'IWM', 'NVDA', 'VIX', 'AAPL', 'F', 'META', 'MSFT', 'GOOGL', 'HYG', 'INTC', 'SQQQ', 'AMD', 'TQQQ', 'XLF', 'BAC', 'XLI', 'TLT', 'GOOG', 'GLD', 'SOFI', 'EEM', 'EFA', 'UVXY', 'NFLX', 'ENPH', 'SQ', 'COIN', 'CVX', 'PLTR', 'XBI', 'FXI', 'XOM', 'VXX', 'PYPL', 'GDX', 'AAL', 'MARA', 'JPM', 'XLE', 'EWZ', 'PFE', 'BABA', 'AMC', 'SLV', 'SOXL', 'DIS', 'UBER', 'DIA', 'GM', 'CVNA', 'RIVN', 'RIOT', 'VALE', 'KRE', 'C', 'VZ', 'USO', 'BA', 'ARKK', 'X', 'MPW', 'XSP', 'NIO', 'SNAP', 'RUT', 'KVUE', 'EDR', 'SHOP', 'SMH', 'BMY', 'JNJ', 'KWEB', 'CHPT', 'MRNA', 'BITO', 'GOLD', 'ZM', 'T', 'NEM', 'ET', 'KO', 'PBR', 'MS', 'SCHW', 'OXY', 'MU', 'DKNG', 'RIG', 'MO', 'WFC', 'NDX', 'VFS', 'XLU', 'BKLN', 'MCD', 'ABBV', 'JBLU', 'FSLR', 'AI', 'LCID', 'SNOW', 'ABNB', 'TNA', 'DVN', 'DAL', 'RTX', 'JD', 'UNG', 'RBLX', 'TGT', 'ADBE', 'UPS', 'WDC', 'LUV', 'TSM', 'UAL', 'PAA', 'ORCL', 'PLUG', 'GS', 'LQD', 'CCL', 'LABU', 'EPD', 'WE', 'AFRM', 'XPO', 'MSOS', 'IBM', 'XLV', 'NKE', 'MSTR', 'COST', 'QCOM', 'HD', 'CSCO', 'AVGO', 'SPXS', 'CLF', 'TFC', 'GME', 'ON', 'CVS', 'CMG', 'SPXU', 'AGNC', 'XLY', 'COF', 'FCX', 'PDD', 'WMT', 'MTCH', 'NEE', 'XOP', 'CRM', 'ROKU', 'MA', 'RUN', 'SBUX', 'PARA', 'SE', 'V', 'SAVE', 'UPST', 'DXCM', 'LLY', 'NCLH', 'ABT', 'AXP', 'ABR', 'CHWY', 'AA', 'DDOG', 'SVXY', 'LYFT', 'RCL', 'HOOD', 'BEKE', 'IBB', 'LI', 'PINS', 'PANW', 'ETSY', 'YINN', 'SAVA', 'OIH', 'WBA', 'TXN', 'FEZ', 'PG', 'CCJ', 'BOIL', 'SMCI', 'ALGN', 'XLP', 'CRWD', 'GE', 'MRVL', 'BX', 'WBD', 'SOXS', 'MRK', 'W', 'UVIX', 'SPXL', 'TZA', 'URNM', 'CAT', 'PEP', 'IMGN', 'XPEV', 'LULU', 'CVE', 'TTD', 'CMCSA', 'BIDU', 'NLY', 'AX', 'XRT', 'AG', 'BYND', 'BRK B', 'HL', 'M', 'NWL', 'SEDG', 'SIRI', 'EBAY', 'FLEX', 'BTU', 'NKLA', 'DISH', 'MDT', 'PSEC', 'VMW', 'ZS', 'COP', 'DG', 'AMAT', 'UCO', 'MDB', 'SLB', 'PTON', 'OKTA', 'U', 'HSBC', 'XHB', 'TMUS', 'UNH', 'OSTK', 'CGC', 'NOW', 'TLRY', 'DOCU', 'TDOC', 'MMM', 'HPQ', 'PCG', 'CHTR', 'Z', 'LOW', 'PENN', 'LMT', 'WOLF', 'KMI', 'VLO', 'SPWR', 'XLK', 'DLTR', 'WHR', 'NVAX', 'ARM', 'JETS', 'VNQ', 'DE', 'DLR', 'NET', 'FAS', 'WPM', 'DASH', 'ACN', 'ASHR', 'FUBO', 'CLX', 'ADM', 'SRPT', 'MRO', 'KGC', 'DPST', 'TWLO', 'AR', 'CNC', 'FDX', 'AMGN', 'VRT', 'CLSK', 'EMB', 'KOLD', 'CD', 'HES', 'SPOT', 'XLC', 'ZIM', 'GILD', 'EQT', 'CRSP', 'GDXJ', 'STNG', 'NAT', 'HAL', 'SGEN', 'GPS', 'USB', 'QS', 'UPRO', 'KSS', 'IDXX', 'FTNT', 'BALL', 'TMF', 'PACW', 'EL', 'MULN', 'NVO', 'GDDY',  'SPCE', 'SNY', 'KEY', 'MGM', 'FREY', 'CZR', 'LVS', 'TTWO', 'LRCX', 'MXEF', 'PAGP', 'ANET', 'VFC', 'GRPN', 'EW', 'BKNG', 'EOSE', 'TMO', 'SPY', 'SPX', 'QQQ', 'VIX', 'IWM', 'TSLA', 'HYG', 'AMZN', 'AAPL', 'BAC', 'XLF', 'TLT', 'SLV', 'EEM', 'F', 'NVDA', 'GOOGL', 'AMD', 'AAL', 'META', 'INTC', 'PLTR', 'C', 'GLD', 'MSFT', 'GDX', 'FXI', 'VALE', 'GOOG', 'XLE', 'SOFI', 'BABA', 'NIO', 'PFE', 'EWZ', 'PYPL', 'T', 'CCL', 'SNAP', 'DIS', 'GM', 'NKLA', 'WFC', 'TQQQ', 'AMC', 'UBER', 'RIVN', 'KRE', 'PBR', 'XOM', 'LCID', 'MARA', 'JPM', 'GOLD', 'ET', 'PLUG', 'JD', 'VZ', 'WBD', 'EFA', 'KVUE', 'RIG', 'SQ', 'CHPT', 'KWEB', 'KO', 'MU', 'BITO', 'TSM', 'SQQQ', 'SHOP', 'DKNG', 'CSCO', 'XLU', 'COIN', 'MPW', 'OXY', 'SOXL', 'FCX', 'RIOT', 'DAL', 'SCHW', 'TLRY', 'BA', 'NFLX', 'UAL', 'SIRI', 'MS', 'AGNC', 'UVXY', 'XBI', 'PARA', 'ARKK', 'CMCSA', 'DVN', 'UNG', 'VXX', 'CVX', 'CLF', 'RBLX', 'PINS', 'XLI', 'SE', 'CVNA', 'QCOM', 'SGEN', 'USO', 'TMF', 'BMY', 'RTX', 'XSP', 'ORCL', 'WBA', 'NKE', 'PDD', 'X', 'KMI', 'GME', 'NCLH', 'NEM', 'SMH', 'MSOS', 'TEVA', 'M', 'XPEV', 'ABBV', 'JETS', 'ABNB', 'MULN', 'JNJ', 'MO', 'CVS', 'AFRM', 'LUV', 'NEE', 'AI', 'SAVE', 'JBLU', 'HOOD', 'ENPH', 'DIA', 'WMT', 'LYFT', 'NU', 'BP', 'XOP', 'ENVX', 'SPCE', 'NOK', 'GRAB', 'BYND', 'ZM', 'SLB', 'NVAX', 'U', 'MRVL', 'CCJ', 'OPEN', 'CRM', 'CGC', 'AA', 'V', 'IBM', 'PTON', 'SBUX', 'LABU', 'TGT', 'STNE', 'BRK B', 'ASHR', 'UPST', 'QS', 'MRK', 'MRNA', 'VFS', 'XHB', 'TMUS', 'SNOW', 'PANW', 'VFC', 'UPS', 'BX', 'DISH', 'USB', 'TFC', 'GE', 'COP', 'LI', 'MET', 'XRT', 'ROKU', 'XLP', 'CHWY', 'FSLR', 'PG', 'XLK', 'FUBO', 'XLV', 'W', 'AMAT', 'GOEV', 'TXN', 'PEP', 'RUN', 'SWN', 'DOW', 'HD', 'GS', 'KGC', 'Z', 'AG', 'ABR', 'CAT', 'UUP', 'AXP', 'ZIM', 'KHC', 'RCL', 'LAZR', 'BOIL', 'DDOG', 'PENN', 'TTD', 'TELL', 'XLY', 'EPD', 'CRWD', 'VMW', 'NYCB', 'HUT', 'BTU', 'DOCU', 'NET', 'BKLN', 'SU', 'BAX', 'ETSY', 'HE', 'BTG', 'NLY', 'BHC', 'TDOC', 'LUMN', 'CLSK', 'MCD', 'LVS', 'MMM', 'DM', 'ALLY', 'SPWR', 'VRT', 'ABT', 'DASH', 'ADBE', 'TNA', 'MA', 'ACB', 'MDT', 'MGM', 'COST', 'WDC', 'GSAT', 'GPS', 'ON', 'MRO', 'PAAS', 'EOSE', 'LQD', 'BILI', 'AR', 'ONON', 'HTZ', 'TWLO', 'GILD', 'MMAT', 'ASTS', 'STLA', 'LLY', 'SABR', 'BIDU', 'EDR', 'AVGO', 'HAL', 'DG', 'WYNN', 'AEM', 'PATH', 'DB', 'IYR', 'UNH', 'HL', 'IEF', 'SPXS', 'CPNG', 'URA', 'NVO', 'BITF', 'URNM', 'KSS',  'KEY', 'TH', 'GEO', 'FDX', 'CL', 'AZN', 'HPQ', 'DNN', 'BSX', 'SHEL', 'DXCM', 'PCG', 'BEKE', 'DNA', 'PM', 'TTWO', 'IQ', 'WE', 'ALB', 'SAVA', 'GDXJ', 'SPXU', 'OSTK', 'COF', 'SNDL', 'OKTA', 'BXMT', 'UEC', 'VLO', 'KR', 'ZION', 'WW', 'RSP', 'XP', 'IAU', 'LULU', 'ARCC', 'SOXS', 'VOD', 'TJX', 'MOS', 'EQT', 'IONQ', 'STNG', 'NOVA', 'HLF', 'HSBC', 'ARM']

    
        #miscellaenous
                #sessions

        self.headers = {
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "access_token": "dc_us_tech1.19157ec9939-8429a39aaed840abb3f8071ffd837935",
            "app": "global",
            "app-group": "broker",
            "appid": "wb_web_app",
            "device-type": "Web",
            "did": "gldaboazf4y28thligawz4a7xamqu91g",
            "hl": "en",
            "origin": "https://app.webull.com",
            "os": "web",
            "osv": "i9zh",
            "platform": "web",
            "priority": "u=1, i",
            "referer": "https://app.webull.com/",
            "reqid": "h7j967zub4iz2okthgrn0n75oxxr6cx8",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "t_time": "1724113378794",
            "tz": "America/Chicago",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
            "ver": "4.9.5",
            "x-s": "38ffe549c7ebc381f361b576ce612aaccd196a1d2584d48336d7da03764c4de9",
            "x-sv": "xodp2vg9"
        }

        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))


    def _get_did(self, path=''):
            '''
            Makes a unique device id from a random uuid (uuid.uuid4).
            if the pickle file doesn't exist, this func will generate a random 32 character hex string
            uuid and save it in a pickle file for future use. if the file already exists it will
            load the pickle file to reuse the did. Having a unique did appears to be very important
            for the MQTT web socket protocol

            path: path to did.bin. For example _get_did('cache') will search for cache/did.bin instead.

            :return: hex string of a 32 digit uuid
            '''
            filename = 'did.bin'
            if path:
                filename = os.path.join(path, filename)
            if os.path.exists(filename):
                did = pickle.load(open(filename,'rb'))
            else:
                did = uuid.uuid4().hex
                pickle.dump(did, open(filename, 'wb'))
            return did
    
    # async def get_token(self):
    #     endpoint = f"https://u1suser.webullfintech.com/api/user/v1/login/account/v2"

    #     async with httpx.AsyncClient(headers=self.headers) as client:
    #         data = await client.post(endpoint, json={"account":"brainfartastic@gmail.com","accountType":"2","pwd":"306a2ecebccfb37988766fac58f9d0e3","deviceId":"gldaboazf4y28thligawz4a7xamqu91g","deviceName":"Windows Chrome","grade":1,"regionId":1})
    #         data = data.json()
    #         token = data.get('accessToken')
    #         return token

    
    # async def get_headers(self):
    #     headers = wb.build_req_headers()
    #     headers.update({"Access_token": await self.get_token()})

        # return headers
    def human_readable(self, string):
        try:
            match = re.search(r'(\w{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)', string) #looks for the options symbol in O: format
            underlying_symbol, year, month, day, call_put, strike_price = match.groups()
                
        except Exception as e:
            underlying_symbol = f"AMC"
            year = "23"
            month = "02"
            day = "17"
            call_put = "CALL"
            strike_price = "380000"
        
        expiry_date = month + '/' + day + '/' + '20' + year
        if call_put == 'C':
            call_put = 'Call'
        else:
            call_put = 'Put'
        strike_price = '${:.2f}'.format(float(strike_price)/1000)
        return "{} {} {} Expiring {}".format(underlying_symbol, strike_price, call_put, expiry_date)
    def sanitize_value(self, value, col_type):
        """Sanitize and format the value for SQL query."""
        if col_type == 'str':
            # For strings, add single quotes
            return f"'{value}'"
        elif col_type == 'date':
            # For dates, format as 'YYYY-MM-DD'
            if isinstance(value, str):
                try:
                    datetime.strptime(value, '%Y-%m-%d')
                    return f"'{value}'"
                except ValueError:
                    raise ValueError(f"Invalid date format: {value}")
            elif isinstance(value, datetime):
                return f"'{value.strftime('%Y-%m-%d')}'"
        else:
            # For other types, use as is
            return str(value)

    async def get_webull_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_id = self.ticker_to_id_map.get(symbol)
        print(ticker_id)
        return ticker_id
    async def get_option_ids(self, ticker, db):
        try:
            async with asyncio.Semaphore(10):
                data = await self.all_options(ticker=ticker)

                data = data[2].as_dataframe

                await db.batch_insert_dataframe(data, table_name='option_ids', unique_columns='option_id')
        except Exception as e:
            print(e)
    async def all_options(self, ticker, direction='all', headers=None):
        try:

            # Calculate the nearest Friday
            today = datetime.now()
            ticker_id = await self.get_webull_id(ticker)
            nearest_friday = today + timedelta((4-today.weekday()) % 7)  # 4 represents Friday
            params = {"type":0,"quoteMultiplier":100,"count":-1,"direction":"all","tickerId":ticker_id,"unSymbol":f"{ticker}"}

                
            


            

            url = "https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"
            

            async with httpx.AsyncClient(headers=self.headers, timeout=60) as client:
                response = await client.post(url, json=params, headers=headers)
                response.raise_for_status()
                data = response.json()
                print(data)
                from_ = 0
                base_data = OptionData(data)
                option_data = OptionData(data)

                return base_data, from_, option_data

        except Exception as e:
            print(e)


    async def all_options_new(self, ticker, days_until_expiry, headers):
        ticker_id = await self.get_webull_id(ticker)
        payload={"filter":{"options.screener.rule.expireDate":f"gte={days_until_expiry}&lte={days_until_expiry}","options.screener.rule.source":[{ticker_id}]},"page":{"fetchSize":200}}
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/option/screener/query"
        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.post(data=payload, url=endpoint)
            data = data.json()
            from_ = 0
            base_data = BaseData(data)
            option_data = OptionData(data)

            return base_data, from_, option_data

    async def update_wb_opts_table(self, buy_vol, neut_vol, sell_vol, trades, total_vol, avg_price, conn, option_symbol):
        update_query = f"""
        UPDATE wb_opts
        SET buy_vol = {buy_vol}, neut_vol = {neut_vol}, sell_vol = {sell_vol}, trades = {trades}, total_vol = {total_vol}, avg_price = {avg_price}
        WHERE option_symbol = '{option_symbol}';
        """
        await conn.execute(update_query)


 
    async def zeroDTE_options(self, ticker, direction='all'):
        
        ticker_id = await self.get_webull_id(ticker)
 


        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": direction,
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with sema:
                url=f"https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"
                async with session.post(url, data=json.dumps(params)) as resp:
                    data = await resp.json()

                    from_ = 0
                    base_data = OptionData(data)


                    underlying_price = base_data.close
                    vol1y = base_data.vol1y

                    option_data = OptionData(data)
                    
                    
        

                    return base_data, from_, option_data



    async def option_chart_data(self, derivative_id, timeframe:str='1m'):
        now_timestamp = int(time.mktime(datetime.utcnow().timetuple()))
        url = f"https://quotes-gw.webullfintech.com/api/quote/option/chart/kdata?derivativeId={derivative_id}&type={timeframe}&count=800&timestamp={now_timestamp}"

        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(url) as resp:
                data = await resp.json()

                data = [i.get('data') for i in data]


                # Assuming data is a list of strings from your original code
                processed_data = process_candle_data(data)

                print(processed_data)

    async def associate_dates_with_data(self, dates, datas):
        if datas is not None and dates is not None:
        # This function remains for your specific data handling if needed
            return [{**data, 'date': date} for date, data in zip(dates, datas)]
        
    async def execute(self, query):
        return await self.fetch(query)


    async def filter_options(self):
        pass

    async def fetch(self, query, params=None, conn=None):
        # Use conn.fetch with query parameters if params are provided
        if params:
            records = await conn.fetch(query, *params)
        else:
            records = await conn.fetch(query)
        return records

    async def fetch_volume_analysis(self, id, symbol, conn):
        endpoint = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={id}"
        print(endpoint)

        async with httpx.AsyncClient(headers=self.headers) as client:
            response = await client.get(endpoint)
            data = response.json()
            datas = data.get('datas')
            if datas:
                avg_price = data.get('avgPrice')
                buy_vol = data.get('buyVolume')
                neutralVolume = data.get('neutralVolume')
                sellVolume = data.get('sellVolume')
                totalNum = data.get('totalNum')
                totalVolume = data.get('totalVolume')

                if avg_price is not None:
                    await self.update_wb_opts_table(buy_vol, neutralVolume, sellVolume, totalNum, totalVolume, avg_price, symbol)

    def dataframe_to_tuples(self, df):
        """
        Converts a Pandas DataFrame to a list of tuples, each tuple representing a row.
        """
        return [tuple(x) for x in df.to_numpy()]
    async def get_volume_analysis(self, ticker):
        
        data, _, option_data = await self.all_options(ticker)
        derivative_data = await self.db_manager.query_derivative_ids_in_price_range(ticker, current_price=data.close)
        async with aiohttp.ClientSession(headers=self.headers) as session:
            
            for id, option_symbol in derivative_data:
                url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={id}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        vol_anal = await resp.json()
                        dates = vol_anal.get('dates')
                        datas = vol_anal.get('datas')

                        if dates is None:
                            dates = []
                        if datas is None:
                            datas = []

                        # Use a different variable name in the inner loop
                        for date, data in zip(dates, datas):
                            components = get_human_readable_string(option_symbol)
                            underlying_symbol = components.get('underlying_symbol')
                            strike_price = float(components.get('strike_price'))
                            expiry = components.get(datetime.strptime(components.get('expiry_date'), '%Y-%m-%d').date())
                            call_put = components.get('call_put')

                            buy = float(data['buy'])
                            sell = float(data['sell'])
                            price = float(data['price'])
                            ratio = round(float(data['ratio'])*100,2)
                            date = pd.to_datetime(date)

                            await self.db_manager.insert_volume_analysis(option_id=id, option_symbol=option_symbol, underlying_symbol=underlying_symbol, strike_price=strike_price, expiry=expiry, call_put=call_put, buy=buy, sell=sell, price=price, ratio=ratio, date=date)

    async def filter_options(self, order_by=None, **kwargs):
        """
        Filters the options table based on provided keyword arguments.
        Usage example:
            await filter_options(strike_price_min=100, strike_price_max=200, call_put='call',
                                 expire_date='2023-01-01', delta_min=0.1, delta_max=0.5)
        """
        # Start with the base query
        query = f"SELECT * FROM public.wb_opts WHERE "
        params = []
        param_index = 1

        # Mapping kwargs to database columns and expected types, including range filters
        column_types = {
            'ticker_id': ('ticker_id', 'int'),
            'belong_ticker_id': ('belong_ticker_id', 'int'),
            'open_min': ('open', 'float'),
            'open_max': ('open', 'float'),
            'open': ('open', 'float'),
            'high_min': ('high', 'float'),
            'high_max': ('high', 'float'),
            'high': ('high', 'float'),
            'low_min': ('low', 'float'),
            'low_max': ('low', 'float'),
            'low': ('low', 'float'),
            'strike_price_min': ('strike_price', 'int'),
            'strike_price_max': ('strike_price', 'int'),
            'strike_price': ('strike_price', 'int'),
            'pre_close_min': ('pre_close', 'float'),
            'pre_close_max': ('pre_close', 'float'),
            'open_interest_min': ('open_interest', 'float'),
            'open_interest_max': ('open_interest', 'float'),
            'volume_min': ('volume', 'float'),
            'volume_max': ('volume', 'float'),
            'latest_price_vol_min': ('latest_price_vol', 'float'),
            'latest_price_vol_max': ('latest_price_vol', 'float'),
            'delta_min': ('delta', 'float'),
            'delta_max': ('delta', 'float'),
            'delta': ('delta', 'float'),
            'vega_min': ('vega', 'float'),
            'vega_max': ('vega', 'float'),
            'imp_vol': ('imp_vol', 'float'),
            'imp_vol_min': ('imp_vol', 'float'),
            'imp_vol_max': ('imp_vol', 'float'),
            'gamma_min': ('gamma', 'float'),
            'gamma_max': ('gamma', 'float'),
            'gamma': ('gamma', 'float'),
            'theta': ('theta', 'float'),
            'theta_min': ('theta', 'float'),
            'theta_max': ('theta', 'float'),
            'rho_min': ('rho', 'float'),
            'rho_max': ('rho', 'float'),
            'close_min': ('close', 'float'),
            'close': ('close', 'float'),
            'close_max': ('close', 'float'),
            'change_min': ('change', 'float'),
            'change_max': ('change', 'float'),
            'change_ratio_min': ('change_ratio', 'float'),
            'change_ratio_max': ('change_ratio', 'float'),
            'change_ratio': ('change_ratio', 'float'),
            'expire_date_min': ('expire_date', 'date'),
            'expire_date_max': ('expire_date', 'date'),
            'expire_date': ('expire_date', 'date'),
            'open_int_change_min': ('open_int_change', 'float'),
            'open_int_change_max': ('open_int_change', 'float'),
            'active_level_min': ('active_level', 'float'),
            'active_level_max': ('active_level', 'float'),
            'cycle_min': ('cycle', 'float'),
            'cycle_max': ('cycle', 'float'),
            'call_put': ('call_put', 'str'),
            'option_symbol': ('option_symbol', 'str'),
            'underlying_symbol': ('underlying_symbol', 'str'),
            'oi_weighted_delta_min': ('oi_weighted_delta', 'float'),
            'oi_weighted_delta_max': ('oi_weighted_delta', 'float'),
            'iv_spread_min': ('iv_spread', 'float'),
            'iv_spread_max': ('iv_spread', 'float'),
            'oi_change_vol_adjusted_min': ('oi_change_vol_adjusted', 'float'),
            'oi_change_vol_adjusted_max': ('oi_change_vol_adjusted', 'float'),
            'oi_pcr_min': ('oi_pcr', 'float'),
            'oi_pcr_max': ('oi_pcr', 'float'),
            'oc_pcr': ('oi_pcr', 'float'),
            'volume_pcr_min': ('volume_pcr', 'float'),
            'volume_pcr_max': ('volume_pcr', 'float'),
            'volume_pcr': ('volume_pcr', 'float'),
            'vega_weighted_maturity_min': ('vega_weighted_maturity', 'float'),
            'vega_weighted_maturity_max': ('vega_weighted_maturity', 'float'),
            'theta_decay_rate_min': ('theta_decay_rate', 'float'),
            'theta_decay_rate_max': ('theta_decay_rate', 'float'),
            'velocity_min': ('velocity', 'float'),
            'velocity_max': ('velocity', 'float'),
            'gamma_risk_min': ('gamma_risk', 'float'),
            'gamma_risk_max': ('gamma_risk', 'float'),
            'delta_to_theta_ratio_min': ('delta_to_theta_ratio', 'float'),
            'delta_to_theta_ratio_max': ('delta_to_theta_ratio', 'float'),
            'liquidity_theta_ratio_min': ('liquidity_theta_ratio', 'float'),
            'liquidity_theta_ratio_max': ('liquidity_theta_ratio', 'float'),
            'sensitivity_score_min': ('sensitivity_score', 'float'),
            'sensitivity_score_max': ('sensitivity_score', 'float'),
            'dte_min': ('dte', 'int'),
            'dte_max': ('dte', 'int'),
            'dte': ('dte', 'int'),
            'time_value_min': ('time_value', 'float'),
            'time_value_max': ('time_value', 'float'),
            'time_value': ('time_value', 'float'),
            'moneyness': ('moneyness', 'str')
        }

        # Dynamically build query based on kwargs
        query = "SELECT * FROM public.wb_opts WHERE open_interest > 0"
        if order_by and isinstance(order_by, list):
                order_clauses = []
                for column, direction in order_by:
                    if column in column_types:  # Ensure the column is valid
                        direction = direction.upper()
                        if direction in ['ASC', 'DESC']:
                            order_clauses.append(f"{column} {direction}")
                if order_clauses:
                    order_by_clause = ', '.join(order_clauses)
                    query += f" ORDER BY {order_by_clause}"
        # Dynamically build query based on kwargs
        for key, value in kwargs.items():
            if key in column_types and value is not None:
                column, col_type = column_types[key]

                # Sanitize and format value for SQL query
                sanitized_value = self.sanitize_value(value, col_type)

                if 'min' in key:
                    query += f" AND {column} >= {sanitized_value}"
                elif 'max' in key:
                    query += f" AND {column} <= {sanitized_value}"
                else:
                    query += f" AND {column} = {sanitized_value}"
                print(query)
        conn = await self.db_manager.get_connection()

        try:
            # Execute the query
            return await conn.fetch(query)
        except Exception as e:
            print(f"Error during query: {e}")
            return []
        
    async def find_extreme_tickers(self, pool):
        # SQL query to find tickers that are overbought or oversold on both day and week timespans
        query_sql = """
        SELECT day_rsi.ticker, day_rsi.status
        FROM rsi as day_rsi
        JOIN rsi as week_rsi ON day_rsi.ticker = week_rsi.ticker
        WHERE day_rsi.timespan = 'day' 
        AND week_rsi.timespan = 'week'
        AND day_rsi.status IN ('overbought', 'oversold')
        AND week_rsi.status IN ('overbought', 'oversold')
        AND day_rsi.status = week_rsi.status;
        """

            # Execute the query using the provided connection pool
        async with pool.acquire() as conn:
            records = await conn.fetch(query_sql)
            return [(record['ticker'], record['status']) for record in records]



    async def find_plays(self):


        async with asyncpg.create_pool(host='localhost', user='chuck', database='market_data', port=5432, password='fud') as pool:
            extreme_tickers_with_status = await self.find_extreme_tickers(pool)

            # To separate the tickers and statuses, you can use list comprehension
            extreme_tickers = [ticker for ticker, status in extreme_tickers_with_status]
            statuses = [status for ticker, status in extreme_tickers_with_status]
            all_options_df_calls =[]
            all_options_df_puts = []
            for ticker, status in extreme_tickers_with_status:
                if status == 'overbought':
                    print(f"Ticker {ticker} is overbought.")
                    all_options = await self.opts.get_option_chain_all(underlying_asset=ticker, expiration_date_gte='2024-03-01', expiration_date_lite='2024-06-30', contract_type='put')
                    
                    for i in range(len(all_options.theta)):  # Assuming all lists are of the same length
                        theta_value = all_options.theta[i]
                        volume = all_options.volume[i]
                        open_interest = all_options.open_interest[i]
                        ask = all_options.ask[i]
                        bid = all_options.bid[i]

                        # Conditions
                        theta_condition = theta_value is not None and theta_value >= -0.03
                        volume_condition = volume is not None and open_interest is not None and volume > open_interest
                        price_condition = ask is not None and bid is not None and 0.25 <= bid <= 1.75 and 0.25 <= ask <= 1.75

                        if theta_condition and volume_condition and price_condition:
                            df = pd.DataFrame([all_options.ticker, all_options.underlying_ticker, all_options.strike, all_options.contract_type, all_options.expiry])
                            all_options_df_puts.append(df)  #

                if status == 'oversold':
                    print(f"Ticker {ticker} is oversold.")
                    all_options = await self.opts.get_option_chain_all(ticker, expiration_date_gte='2024-03-01', expiration_date_lte='2024-11-30', contract_type='call')
                    
                    for i in range(len(all_options.theta)):  # Assuming all lists are of the same length
                        theta_value = all_options.theta[i]
                        volume = all_options.volume[i]
                        open_interest = all_options.open_interest[i]
                        ask = all_options.ask[i]
                        bid = all_options.bid[i]

                        # Conditions
                        theta_condition = theta_value is not None and theta_value >= -0.03
                        volume_condition = volume is not None and open_interest is not None and volume > open_interest
                        price_condition = ask is not None and bid is not None and 0.25 <= bid <= 1.75 and 0.25 <= ask <= 1.75

                        if theta_condition and volume_condition and price_condition:
                            # Assuming all_options.df is a DataFrame containing the current option data
                            df = pd.DataFrame([all_options.ticker, all_options.strike, all_options.contract_type, all_options.expiry])
                            all_options_df_calls.append(df)  #
            # Concatenate all the dataframes
            final_df_calls = pd.concat(all_options_df_calls, ignore_index=True)
            final_df_puts = pd.concat(all_options_df_puts, ignore_index=True)
            print(final_df_calls, final_df_puts)
            return final_df_calls, final_df_puts, extreme_tickers, statuses
        

    async def yield_batch_ids(self, ticker_symbol):
        conn = await self.db_manager.get_connection()

        # We will fetch all derivative IDs associated with the ticker symbol
        derivative_ids = await conn.fetch(
            'SELECT ticker_id FROM wb_opts WHERE underlying_symbol = $1',
            ticker_symbol
        )
        
        # Convert the records to a list of IDs
        derivative_id_list = [str(record['ticker_id']) for record in derivative_ids]

        # Yield batches of 55 IDs at a time as a comma-separated string
        for i in range(0, len(derivative_id_list), 55):
            yield ','.join(derivative_id_list[i:i+55])

    async def get_option_ids(self, ticker):
        ticker_id = await trading.get_webull_id(ticker)
        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": "all",
            "expireCycle": [1,
                3,
                2,
                4
            ],
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        data = json.dumps(params)
        url="https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"

        # Headers you may need to include, like authentication tokens, etc.
        headers = trading.headers
        # The body of your POST request as a Python dictionary
        import pandas as pd
        # Make the POST request
        # Make the POST request
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, data=data) as resp:
                response_json = await resp.json()
             
                # Extract the 'expireDateList' from the response
                expireDateList = response_json.get('expireDateList')

                # Flatten the nested 'data' from each item in 'expireDateList'
            try:
                data_flat = [item for sublist in expireDateList if sublist and sublist.get('data') for item in sublist['data']]



                # Create a DataFrame from the flattened data
                df_cleaned = pd.DataFrame(data_flat)

                # Drop the 'askList' and 'bidList' columns if they exist
                df_cleaned.drop(columns=['askList', 'bidList'], errors='ignore', inplace=True)
                # Existing DataFrame columns
                df_columns = df_cleaned.columns

                # Original list of columns you want to convert to numeric
                numeric_cols = ['open', 'high', 'low', 'strikePrice', 'isStdSettle', 'quoteMultiplier', 'quoteLotSize']

                # Filter the list to include only columns that exist in the DataFrame
                existing_numeric_cols = [col for col in numeric_cols if col in df_columns]

                # Now apply the to_numeric conversion only to the existing columns
                df_cleaned[existing_numeric_cols] = df_cleaned[existing_numeric_cols].apply(pd.to_numeric, errors='coerce')

      
   
                df_cleaned.to_csv('test.csv', index=False)


                # Load the data from the CSV file
                df = pd.read_csv('test.csv')

                # Extract 'tickerId' column values in batches of 55
                ticker_ids = df['tickerId'].unique()  # Assuming 'tickerId' is a column in your DataFrame
                symbol_list = df['symbol'].unique().tolist()
            # Pair up 'tickerId' and 'symbol'
                # Before you call batch_insert_options, make sure pairs contain the correct types
                pairs = [(str(symbol), int(ticker_id), str(ticker)) for ticker_id, symbol in zip(ticker_ids, symbol_list)]

                
                await self.batch_insert_options(pairs)

                return ticker_ids


               
            except (ContentTypeError, TypeError):
                print(f'Error for {ticker}')
    async def update_and_insert_options(self, ticker):

        data, _, options = await self.all_options(ticker)

        
   


        df = options.as_dataframe
        df['symbol_string'] = df['option_symbol'].apply(human_readable)


        # Assuming opts.db_manager.get_connection() returns a connection,
        await self.db_manager.batch_insert_wb_dataframe(df, table_name='wb_opts', history_table_name='wb_opts_history')

    async def update_all_options(self):
        await self.db_manager.get_connection()

        tasks = [self.update_and_insert_options(i) for i in self.most_active_tickers]

        await asyncio.gather(*tasks)






    async def get_option_ids_limited(self, sem, ticker):
        async with sem:
            # This will wait until the semaphore allows entry (i.e., under the limit)
            return await self.get_option_ids(ticker)


    async def batch_insert_options(self, pairs):
        try:
            conn = await self.db_manager.get_connection()  # Acquire a connection from the pool

            async with conn.transaction():  # Start a transaction
                # Prepare the statement to insert data
                insert_query = 'INSERT INTO wb_opts (underlying_symbol, ticker_id, option_symbol) VALUES ($1, $2, $3)'
                # Perform the batch insert
                await conn.executemany(insert_query, pairs)
                print("Batch insert completed.")
        except asyncpg.exceptions.UniqueViolationError:
            print(f'Duplicate found - skipping.')


    async def get_option_id_for_symbol(self, ticker_symbol):
        async with self.pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Execute the query to get the option_id for a given ticker_symbol
                # This assumes 'symbol' column exists in 'options_data' table and 
                # is used to store the ticker symbol
                query = f'''
                    SELECT ticker_id FROM wb_opts
                    WHERE ticker = '{ticker_symbol}';
                '''
                # Fetch the result
                result = await conn.fetch(query)
                # Return a list of option_ids or an empty list if none were found
                return [record['ticker_id'] for record in result]


    async def get_option_symbols_by_ticker_id(self, ticker_id):
        async with self.pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Execute the query to get all option_symbols for a given ticker_id
                query = '''
                    SELECT option_symbol FROM wb_opts
                    WHERE ticker_id = $1;
                '''
                # Fetch the result
                records = await conn.fetch(query, ticker_id)
                # Extract option_symbols from the records
                return [record['option_symbol'] for record in records]
    async def get_ticker_symbol_pairs(self):
        # Assume 'pool' is an instance variable pointing to a connection pool
        conn = await self.db_manager.get_connection()
        # Start a transaction
        async with conn.transaction():
            # Create a cursor for iteration using 'cursor()' instead of 'execute()'
            async for record in conn.cursor('SELECT ticker_id, symbol FROM webull_opts'):
                yield (record['ticker_id'], record['symbol'])

    async def option_volume_analysis(self, ticker):
        ticker_id = await trading.get_webull_id(ticker)
        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": "all",
            "expireCycle": [1,
                3,
                2,
                4
            ],
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        data = json.dumps(params)
        url="https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"

        # Headers you may need to include, like authentication tokens, etc.
        headers = trading.headers
        # The body of your POST request as a Python dictionary
        import pandas as pd
        # Make the POST request
        # Make the POST request
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, data=data) as resp:
                response_json = await resp.json()
          
                # Extract the 'expireDateList' from the response
                expireDateList = response_json.get('expireDateList')

                # Flatten the nested 'data' from each item in 'expireDateList'
            try:
                data_flat = [item for sublist in expireDateList if sublist and sublist.get('data') for item in sublist['data']]



                # Create a DataFrame from the flattened data
                df_cleaned = pd.DataFrame(data_flat)

                # Drop the 'askList' and 'bidList' columns if they exist
                df_cleaned.drop(columns=['askList', 'bidList'], errors='ignore', inplace=True)

                # Convert specified columns to numeric values, coercing errors to NaN
                numeric_cols = ['open', 'high', 'low', 'strikePrice', 'isStdSettle', 'quoteMultiplier', 'quoteLotSize']
                # Iterate through the list of numeric columns and check if they exist in df_cleaned
                existing_numeric_cols = [col for col in numeric_cols if col in df_cleaned.columns]

                # Now apply the conversion only on the columns that exist
                df_cleaned[existing_numeric_cols] = df_cleaned[existing_numeric_cols].apply(pd.to_numeric, errors='coerce')

      
                df_cleaned.to_csv('test.csv', index=False)


                # Load the data from the CSV file
                df = pd.read_csv('test.csv')

                # Extract 'tickerId' column values in batches of 55
                ticker_ids = df['tickerId'].unique()  # Assuming 'tickerId' is a column in your DataFrame
                symbol_list = df['symbol'].unique().tolist()
            # Pair up 'tickerId' and 'symbol'
                pairs = list(zip(ticker_ids, symbol_list))

                
                # Split into batches of 55
                batches = [ticker_ids[i:i + 55] for i in range(0, len(ticker_ids), 55)]

                ticker_id_strings = [','.join(map(str, batch)) for batch in batches]







                for ticker_id_string in ticker_id_strings:
                    ticker_ids = ticker_id_string.split(',')
                    for deriv_id in ticker_ids:
                        all_data = []
                        volume_analysis_url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={deriv_id}"
                        async with aiohttp.ClientSession(headers=headers) as session:
                            async with session.get(volume_analysis_url) as resp:
                                data = await resp.json()
                                all_data.append(data)


                   
                        return all_data
                        #df = pd.DataFrame(all_data)
                        #df.to_csv('all_options', index=False)
            except (ContentTypeError, TypeError):
                print(f'Error for {ticker}')


    async def harvest_options(self,most_active_tickers):
        # Set the maximum number of concurrent requests
        max_concurrent_requests = 5  # For example, limiting to 10 concurrent requests

        # Create a semaphore with your desired number of concurrent requests
        sem = asyncio.Semaphore(max_concurrent_requests)
        await self.connect()
        # Create tasks using the semaphore
        tasks = [self.get_option_ids_limited(sem, ticker) for ticker in most_active_tickers]

        # Run the tasks concurrently and wait for all to complete
        await asyncio.gather(*tasks)


    async def get_option_data(self, info):
        url = f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={info}"
        async with aiohttp.ClientSession(headers=trading.headers) as session:
            async with session.get(url) as resp:
                data = await resp.json()
                wb_data = OptionData(data)
                return wb_data
            

    async def option_flow(self, option_id):   

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=350&tickerId={option_id}")
            response = data.json()
            tickerId = response.get('tickerId')
            belongTickerId = response.get('belongTickerId')
            lastTimestamp= response.get('lastTimestamp')
            timeZone= response.get('timeZone')
            datas= response.get('datas')
            tradeTime = [i.get('tradeTime') for i in datas]
            deal = [i.get('deal') for i in datas]
            volume = [i.get('volume') for i in datas]
            tradeBsFlag = [i.get('tradeBsFlag') for i in datas]
            tid = [i.get('tid') for i in datas]


            data_dict = { 
                'option_id': option_id,
                'time': tradeTime,
                'deal': deal,
                'volume': volume,
                'trade_flag': tradeBsFlag,
                'tid': tid
            }

            df = pd.DataFrame(data_dict)



            return df
        
    async def atm_options(self, ticker:str, lower_strike:int=0.95, upper_strike:int=0.95, limit:int=25):
        """Get ATM options for a ticker."""

        base, from_, options = await self.all_options(ticker)
        df = options.as_dataframe
        df['symbol_string'] = df['option_symbol'].apply(human_readable)
        await self.db_manager.batch_insert_dataframe(df, table_name='wb_opts', unique_columns='option_symbol')

        price = base.under_close


        lower_strike  = float(price) * 0.95
        upper_strike = float(price) * 1.05


        query = f"""SELECT ticker, strike, cp, expiry, vol, oi, oi_change, vega, theta, delta, gamma, option_id FROM wb_opts WHERE strike >= {lower_strike} and strike <= {upper_strike} and ticker = '{ticker}' order by expiry ASC LIMIT {limit};"""


        results = await self.db_manager.fetch(query)

        df = pd.DataFrame(results, columns=['ticker', 'strike', 'cp', 'expiry', 'vol', 'oi', 'oi_change', 'vega', 'theta', 'delta', 'gamma', 'id'])


        return df
        


    async def vol_anal_new(self, ticker):
        await self.db_manager.get_connection()
        data = await self.atm_options(ticker=ticker)
        all_parsed_data = []
        for i, row in data.iterrows():
            option_id = row['id']
            url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={option_id}"
            async with httpx.AsyncClient(headers=self.headers) as client:
                response = await client.get(url)
                data = response.json()

                if 'dates' in data:
                    for date in data['dates']:
                        entry = data['datas'][0] if 'datas' in data and len(data['datas']) > 0 else {}
                        entry['date'] = date
                        entry['option_id'] = option_id
                        all_parsed_data.append(entry)
                else:
                    # If there are no 'dates', use the first data entry and today's date
                    today = datetime.strptime(self.today, "%Y-%m-%d").strftime("%Y-%m-%d")
                    entry = data['datas'][0] if 'datas' in data and len(data['datas']) > 0 else {}
                    entry['date'] = today
                    entry['option_id'] = option_id
                    all_parsed_data.append(entry)

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(all_parsed_data).drop_duplicates()
        
        # Flatten the DataFrame if necessary and perform any additional formatting
        # This step depends on the structure of your 'entry' dictionaries

        return df



    async def stream_options(self, option_id):

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=500&tickerId={option_id}")
            response = data.json()
            tickerId = response.get('tickerId')
            belongTickerId = response.get('belongTickerId')
            lastTimestamp= response.get('lastTimestamp')
            timeZone= response.get('timeZone')
            datas= response.get('datas')
            tradeTime = [i.get('tradeTime') for i in datas]
            deal = [i.get('deal') for i in datas]
            volume = [i.get('volume') for i in datas]
            tradeBsFlag = [i.get('tradeBsFlag') for i in datas]
            tid = [i.get('tid') for i in datas]


            data_dict = { 
                'time': tradeTime,
                'deal': deal,
                'volume': volume,
                'trade_flag': tradeBsFlag,
                'tid': tid
            }

            df = pd.DataFrame(data_dict)
            print(df)
            return df


    async def order_flow(self, option_id):
        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=800&tickerId={option_id}")

            data = data.json()

            last_time = data.get('lastTimestamp')
            datas = data.get('datas')

            volume = [i.get('volume') for i in datas]
            tradeBsFlag = [i.get('tradeBsFlag') for i in datas]
            tid = [i.get('tid') for i in datas]
            trdEx = [i.get('trdEx') for i in datas]

            data_dict = { 
                'volume': volume,
                'side': tradeBsFlag,
                'id': tid,
                'exchange': trdEx
            }


            df = pd.DataFrame(data_dict)




            return df
            

    async def get_iv_skew(self, ticker: str):
        current_price = await self.db.get_price(ticker)
        _, __, options = await self.all_options(ticker)

        df = options.as_dataframe


        # Ensure the DataFrame contains necessary columns
        if 'iv' not in df.columns or 'expiry' not in df.columns or 'strike' not in df.columns:
            raise ValueError("DataFrame does not contain required columns: 'iv', 'expiry', 'strike'")

        results = []

        # Group by expiry and find the strike with the lowest IV for each group
        grouped = df.groupby('expiry')
        for expiry, group in grouped:
            print(f"Processing expiry: {expiry}")
            print(group[['strike', 'iv']])  # Debugging: print the group being processed

            min_iv_row = group.loc[group['iv'].idxmin()]
            lowest_iv_strike = min_iv_row['strike']
            
            print(f"Lowest IV strike for expiry {expiry}: {lowest_iv_strike}")  # Debugging: print the lowest IV strike

            if lowest_iv_strike < current_price:
                position = 'below'
            elif lowest_iv_strike > current_price:
                position = 'above'
            else:
                position = 'at'
            
            results.append({
                'expiry': expiry,
                'lowest_iv_strike': lowest_iv_strike,
                'current_price': current_price,
                'position': position
            })
        
        return results
    
    async def process_ids(self, ids, symbols):
        symbols = [get_human_readable_string(i) for i in symbols]
        async with httpx.AsyncClient(headers=self.headers) as client:
            tasks = [client.get(f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={str(id)}") for id in ids]
            responses = await asyncio.gather(*tasks)
            
            # Parse the responses
            response = [i.json() for i in responses]

            # Extract attributes from the responses
            ticker_id = [i.get('belongTickerId') for i in response]
            option_id = [i.get('tickerId') for i in response]
            trades = [i.get('totalNum') for i in response]
            volume = [i.get('totalVolume') for i in response]
            avg_price = [i.get('avgPrice') for i in response]
            buy_vol = [i.get('buyVolume') for i in response]
            sell_vol = [i.get('sellVolume') for i in response]
            neut_vol = [i.get('neutralVolume') for i in response]

            # Create a mapping of ids to symbols
            id_to_symbol = dict(zip(ids, symbols))
            # Prepare the fields for the DataFrame
            underlying_ticker = [id_to_symbol.get(id, {}).get('underlying_symbol', 'Unknown') for id in option_id]
            strike_price = [id_to_symbol.get(id, {}).get('strike_price', 'Unknown') for id in option_id]
            expiry_date = [id_to_symbol.get(id, {}).get('expiry_date', 'Unknown') for id in option_id]
            call_put = [id_to_symbol.get(id, {}).get('call_put', 'Unknown') for id in option_id]

    


            # Create the DataFrame
            data_dict = {
                'sym': underlying_ticker,
                'strike': strike_price,
                'exp': expiry_date,
                'cp': call_put,
                'id': ticker_id,
                'option_id': option_id,
                'trades': trades,
                'volume': volume,
                'avg_price': avg_price,
                'buy_vol': buy_vol,
                'neut_vol': neut_vol,
                'sell_vol': sell_vol
            }

            df = pd.DataFrame(data_dict)
            return df