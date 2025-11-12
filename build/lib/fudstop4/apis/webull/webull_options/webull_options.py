import os
from dotenv import load_dotenv
import numpy as np
load_dotenv()
import json
import pickle
import uuid
import re
from typing import AsyncGenerator
import asyncio
import httpx
import aiohttp
from typing import List, Any
import pandas as pd
from datetime import datetime
import asyncpg
import time
from fudstop4.apis.webull.webull_trading import WebullTrading
trading = WebullTrading()
from webull import webull
wb = webull()
from .models.options_data import MultiOptions
from .models.options_data import From_, GroupData, BaseData, OptionData
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(database='fudstop3')
from fudstop4.apis.helpers import human_readable
from typing import List, Dict
from aiohttp.client_exceptions import ContentTypeError
from .helpers import process_candle_data, get_human_readable_string
from asyncio import Semaphore
from datetime import timedelta
sema = Semaphore(10)

class WebullOptions:
    def __init__(self, database:str='fudstop3', user:str='chuck'):
        self.db = PolygonOptions(database='fudstop3')
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
        self.opts = PolygonOptions(host='localhost', user='chuck', database='fudstop3', password='fud', port=5432)
    
        #miscellaenous
                #sessions
        self.conn: asyncpg.Connection | None = None


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
        

    async def yield_price(self, ticker):
        ticker_id = self.ticker_to_id_map.get(ticker)
        url = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_id}&includeSecu=1&delay=0&more=1"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                close = [i.get('close') for i in data]

                yield close[0]

    async def get_webull_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_id = self.ticker_to_id_map.get(symbol)

        return ticker_id

    async def all_options(self, ticker, direction='all', headers=None):
        try:

            # Calculate the nearest Friday
            today = datetime.now()
            ticker_id = self.ticker_to_id_map.get(ticker)
            nearest_friday = today + timedelta((4-today.weekday()) % 7)  # 4 represents Friday
            params = {"type":0,"quoteMultiplier":100,"count":-1,"direction":"all","tickerId":ticker_id,"unSymbol":f"{ticker}"}

                
            


            

            url = "https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"
            

            async with httpx.AsyncClient(headers=headers, timeout=60) as client:
                response = await client.post(url, json=params, headers=headers)
                response.raise_for_status()
                data = response.json()
           
                from_ = 0
                base_data = OptionData(data)
                option_data = OptionData(data)

                return base_data, from_, option_data

        except Exception as e:
            print(e)


    async def iv_skew(self, ticker, headers):
        # Get current price from yield_price
        current_price = None
        async for price in self.yield_price(ticker):
            current_price = price


        if current_price is None:
            raise ValueError(f"Could not retrieve current price for ticker {ticker}")

        # Fetch all options for the ticker.
        all_opts = await self.all_options(ticker=ticker, headers=headers)
        all_opts = all_opts[2].as_dataframe
        all_opts['underlying_price'] = float(price)
        all_opts['iv'] = all_opts['iv'].astype(float)
        all_opts['strike'] = all_opts['strike'].astype(float)
        # Select only the needed columns (including underlying_price)
        all_opts = all_opts[['ticker', 'strike', 'cp', 'expiry', 'underlying_price', 'iv']]


        all_opts['iv'] = pd.to_numeric(all_opts['iv'], errors='coerce')
        all_opts['strike'] = pd.to_numeric(all_opts['strike'], errors='coerce')
        all_opts['underlying_price'] = pd.to_numeric(all_opts['underlying_price'], errors='coerce')
        # Filter out rows with non-positive IV values.
        all_opts = all_opts[all_opts['iv'] > 0]

        # Sort options by iv, lowest first.
        all_opts = all_opts.sort_values(by="iv", ascending=True)

        # Select the row with the lowest IV.
        lowest_iv_option = all_opts.iloc[[0]][['ticker', 'strike', 'cp', 'expiry', 'underlying_price']]



        # Compute skew type and skew difference.
        strike_val = lowest_iv_option.iloc[0]['strike']

        if strike_val > float(price):
            skew_type = "call"
            skew_diff = strike_val - float(price)
        elif strike_val < float(price):
            skew_type = "put"
            skew_diff = float(price) - strike_val
        else:
            skew_type = "atm"
            skew_diff = 0

        # Add the computed columns to the DataFrame.
        lowest_iv_option["skew_type"] = skew_type
        lowest_iv_option["skew_diff"] = skew_diff

        return lowest_iv_option

    async def all_skew(self, tickers: list[str], headers: dict) -> pd.DataFrame:
        """
        Fetches the IV skew for a list of tickers concurrently.
        
        Args:
            tickers (list[str]): List of ticker symbols.
            headers (dict): HTTP headers to pass to the all_options method.
        
        Returns:
            pd.DataFrame: A DataFrame concatenating the lowest-IV option data (with skew info)
                          for each ticker.
        """
        tasks = [asyncio.create_task(self.iv_skew(ticker, headers)) for ticker in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = []
        for ticker, result in zip(tickers, results):
            if isinstance(result, Exception):
                print(f"Error fetching IV skew for ticker {ticker}: {result}")
            elif result is not None:
                valid_results.append(result)

        if valid_results:
            combined_df = pd.concat(valid_results, ignore_index=True)
        else:
            combined_df = pd.DataFrame()
        return combined_df
    


    async def update_wb_opts_table(self, buy_vol, neut_vol, sell_vol, trades, total_vol, avg_price, conn, option_symbol):
        update_query = f"""
        UPDATE wb_opts
        SET buy_vol = {buy_vol}, neut_vol = {neut_vol}, sell_vol = {sell_vol}, trades = {trades}, total_vol = {total_vol}, avg_price = {avg_price}
        WHERE option_symbol = '{option_symbol}';
        """
        await conn.execute(update_query)


 
    async def zeroDTE_options(self, ticker, direction='all', headers=None):
        
        ticker_id = self.ticker_to_id_map.get(ticker)
 


        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": direction,
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        async with aiohttp.ClientSession(headers=headers) as session:
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



    async def option_chart_data(self, derivative_id, timeframe:str='1m', headers=None):
        now_timestamp = int(time.mktime(datetime.utcnow().timetuple()))
        url = f"https://quotes-gw.webullfintech.com/api/quote/option/chart/kdata?derivativeId={derivative_id}&type={timeframe}&count=800&timestamp={now_timestamp}"

        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url) as resp:
                data = await resp.json()

                data = [i.get('data') for i in data]


                # Assuming data is a list of strings from your original code
                processed_data = process_candle_data(data)

    

    async def associate_dates_with_data(self, dates, datas):
        if datas is not None and dates is not None:
        # This function remains for your specific data handling if needed
            return [{**data, 'date': date} for date, data in zip(dates, datas)]
        
    async def execute(self, query):
        return await self.fetch(query)



    async def fetch(self, query, params=None, conn=None):
        # Use conn.fetch with query parameters if params are provided
        if params:
            records = await conn.fetch(query, *params)
        else:
            records = await conn.fetch(query)
        return records

    async def fetch_volume_analysis(self, id, symbol, headers=None):
        endpoint = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={id}"

        async with httpx.AsyncClient(headers=headers) as client:
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


        async with asyncpg.create_pool(host='localhost', user='chuck', database='fudstop3', port=5432, password='fud') as pool:
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

    async def option_volume_analysis(self, ticker, headers=None):
        ticker_id = self.ticker_to_id_map.get(ticker)
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
            

    async def option_flow(self, option_id, headers=None):   

        async with httpx.AsyncClient(headers=headers) as client:
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
        


    async def vol_anal_new(self, ticker, headers=None):
        data = await self.atm_options(ticker=ticker)
        all_parsed_data = []
        for i, row in data.iterrows():
            option_id = row['id']
            url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={option_id}"
            async with httpx.AsyncClient(headers=headers) as client:
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



    async def stream_options(self, option_id, headers=None):

        async with httpx.AsyncClient(headers=headers) as client:
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
      
            return df


    async def order_flow(self, option_id, headers=None):
        async with httpx.AsyncClient(headers=headers) as client:
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
            
    async def process_ids(self, ids, symbols, headers=None):
        symbols = [get_human_readable_string(i) for i in symbols]
        async with httpx.AsyncClient(headers=headers) as client:
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
        



    async def multi_options(self, ticker, headers=None):
        try:
            x = await self.all_options(ticker, headers=headers)

            ids = x[2].tickerId  # Assuming this is a list of integers
            belong_id = x[2].belongTickerId[0]

            # Split `ids` into chunks of 55
            batch_size = 55
            id_batches = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
            async def fetch_data(ids_batch, belong_id, session):
                """Fetch option quotes for a batch of 55 IDs."""
                ids_str = ','.join(map(str, ids_batch))
                url = f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/detail?derivativeIds={ids_str}&tickerId={belong_id}"
            
                
                async with session.get(url) as resp:
                    return await resp.json()
            async with aiohttp.ClientSession(headers=headers) as session:
                tasks = [fetch_data(batch, belong_id, session) for batch in id_batches]
                responses = await asyncio.gather(*tasks)

            # Process all responses
            derivativeList = [i.get('derivativeList') for i in responses]
            derivativeList = [item for sublist in derivativeList for item in sublist]
            return MultiOptions(derivativeList)
        except Exception as e:
            print(e)

    async def fetch_option_data(self, session, option_id):
        """Fetches option data for a single option_id asynchronously."""
        url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=1000&tickerId={option_id}"
        async with session.get(url) as resp:
            return await resp.json()


    async def all_options_generator(
        self,
        chunk_size: int = 5000,
        start_id: int = 0
    ) -> AsyncGenerator[pd.DataFrame, None]:
        """
        Streams options data using keyset pagination (option_id > last_seen_id).
        This version fetches in ascending order by primary key.
        """
        conn = await asyncpg.connect("postgresql://chuck:fud@localhost:5432/fudstop3")
        column_names = await self.db.get_table_columns('wb_opts')
        last_seen_id = start_id

        while True:
            query = f"""
            SELECT * FROM wb_opts
            WHERE option_id > {last_seen_id}
            ORDER BY option_id ASC
            LIMIT {chunk_size}
            """
            results = await conn.fetch(query)

            if not results:
                break

            df_chunk = pd.DataFrame(results, columns=column_names)
            yield df_chunk

            # advance the pagination
            last_seen_id = df_chunk["option_id"].iloc[-1]

        await conn.close()
        
    async def atm_options(self, ticker:str, lower_strike:int=0.95, upper_strike:int=0.95, limit:int=25):
        """Get ATM options for a ticker."""

        base, from_, options = await self.all_options(ticker)
        df = options.as_dataframe
        df['symbol_string'] = df['option_symbol'].apply(human_readable)
        await self.db.batch_insert_dataframe(df, table_name='wb_opts', unique_columns='option_symbol')

        price = base.under_close


        lower_strike  = float(price) * 0.95
        upper_strike = float(price) * 1.05


        query = f"""SELECT ticker, strike, cp, expiry, vol, oi, oi_change, vega, theta, delta, gamma, option_id FROM wb_opts WHERE strike >= {lower_strike} and strike <= {upper_strike} and ticker = '{ticker}' order by expiry ASC LIMIT {limit};"""


        results = await self.db.fetch(query)

        df = pd.DataFrame(results, columns=['ticker', 'strike', 'cp', 'expiry', 'vol', 'oi', 'oi_change', 'vega', 'theta', 'delta', 'gamma', 'id'])


        return df
        




    async def get_all_options_for_ticker(
        self,
        tickers: List[str],
        headers: Dict[str, str] = None,
        max_concurrent: int = 25
    ) -> Dict[str, MultiOptions]:
        """
        Nested function that:
         - Takes a list of ticker symbols.
         - Uses concurrency (async/await + asyncio.Semaphore) to limit how many tickers
           can be processed simultaneously to 'max_concurrent'.
         - Calls self.multi_options for each ticker to get all derivative data.
         - Returns a dictionary keyed by ticker symbol, each containing a MultiOptions object.
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_ticker(ticker_symbol: str):
            """Fetch data for a single ticker within concurrency limit."""
            async with semaphore:
                return await self.multi_options(ticker_symbol, headers=headers)

        # Create tasks for each ticker
        tasks = [fetch_ticker(t) for t in tickers]

        # Fetch all results concurrently but within the concurrency limit
        results = await asyncio.gather(*tasks)

        # Build a dictionary {ticker: MultiOptions(...), ...}
        ticker_options_map = {}
        for ticker_symbol, multi_opts in zip(tickers, results):
            ticker_options_map[ticker_symbol] = multi_opts

        return ticker_options_map
    

    async def target_strike(self, ticker:str, strike_price:str, headers):
        """Get trade analysis for a strike"""

        x = await self.multi_options(ticker=ticker, headers=headers)
        df = x.as_dataframe

        if not {'strike', 'expiry', 'option_id', 'call_put'}.issubset(df.columns):
            print(f"Error: Missing required columns in dataframe for {ticker}")
            return

        strike_price = float(strike_price)
        strike_df = df[df['strike'] == strike_price].reset_index(drop=True)

        if strike_df.empty:
            print(f"No data found for strike {strike_price} in {ticker}.")
            return

        # Separate Calls and Puts
        calls_df = strike_df[strike_df['call_put'] == 'call']
        puts_df = strike_df[strike_df['call_put'] == 'put']

        # Get option IDs separately for calls and puts
        call_option_ids = calls_df['option_id'].dropna().astype(str).tolist()
        put_option_ids = puts_df['option_id'].dropna().astype(str).tolist()

        async def fetch_option_data(session, option_id):
            """Fetches option data for a single option_id asynchronously."""
            url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=1000&tickerId={option_id}"
            async with session.get(url) as resp:
                return await resp.json()

        # Process all IDs concurrently
        async with aiohttp.ClientSession(headers=headers) as session:
            call_tasks = [fetch_option_data(session, option_id) for option_id in call_option_ids]
            put_tasks = [fetch_option_data(session, option_id) for option_id in put_option_ids]

            call_results = await asyncio.gather(*call_tasks)
            put_results = await asyncio.gather(*put_tasks)

        # Flatten trade data
        call_datas = [i.get('datas', []) for i in call_results if isinstance(i, dict)]
        put_datas = [i.get('datas', []) for i in put_results if isinstance(i, dict)]
        call_datas = [item for sublist in call_datas for item in sublist]
        put_datas = [item for sublist in put_datas for item in sublist]

        trade_data = {f"{t} Calls": {"total_volume": 0, "total_value": 0, "transaction_count": 0} for t in ["Buy", "Sell", "Neutral", "Unknown"]}
        trade_data.update({f"{t} Puts": {"total_volume": 0, "total_value": 0, "transaction_count": 0} for t in ["Buy", "Sell", "Neutral", "Unknown"]})

        def process_trade_data(data, suffix):
            """Processes trade data and updates the trade_data dictionary."""
            for i in data:
                price = i.get('deal')
                volume = i.get('volume')
                flag = i.get('tradeBsFlag')

                try:
                    price = float(price) if price is not None and price != "" else None
                    volume = float(volume) if volume is not None and volume != "" else None
                except ValueError:
                    continue

                if price is None or volume is None:
                    continue

                trade_type = "Buy" if flag == "B" else "Sell" if flag == "S" else "Neutral" if flag == "N" else "Unknown"
                trade_key = f"{trade_type} {suffix}"

                trade_data[trade_key]["total_volume"] += volume
                trade_data[trade_key]["total_value"] += price * volume
                trade_data[trade_key]["transaction_count"] += 1

        # Process Calls and Puts separately
        process_trade_data(call_datas, "Calls")
        process_trade_data(put_datas, "Puts")

        # Compute average prices
        trade_summary = {}
        for trade_type, data in trade_data.items():
            total_volume = data["total_volume"]
            total_value = data["total_value"]
            avg_price = total_value / total_volume if total_volume > 0 else 0

            trade_summary[f"{trade_type} Volume"] = total_volume
            trade_summary[f"{trade_type} Avg Price"] = round(avg_price, 2)
            trade_summary[f"{trade_type} Transactions"] = data["transaction_count"]

        df_summary = pd.DataFrame([trade_summary])

        df_summary = df_summary.rename(columns={'Buy Calls Volume': 'call_buy_vol', 'Buy Calls Avg Price': 'bought_calls_avg_price', 'Buy Calls Transactions': 'bought_call_transactions', 'Sell Calls Volume': 'call_sell_vol', 'Sell Calls Avg Price':'sold_call_avg_price','Neutral Puts Avg Price': 'neutral_put_avg_price', 'Neutral Puts Transactions': 'neutral_put_transactions', 'Unknown Puts Volume': 'put_unknown_vol', 'Unknown Puts Avg Price': 'unknown_put_avg_price', 'Unknown Calls Volume': 'call_unknown_vol', 'Neutral Calls Transactions': 'neutral_call_transactions', 'Sell Puts Transactions': 'sold_put_transactions', 'Sell Calls Transactions': 'sold_call_transactions', 'Neutral Calls Avg Price': 'neutral_call_avg_price', 'Neutral  Calls Volume': 'call_neutral_vol', 'Unknown Calls Avg Price': 'unknown_call_avg_price', 'Sell Puts Avg Price': 'sold_put_avg_price', 'Unknown Calls Transactions': 'unknown_call_transactions', 'Buy Puts Transactions': 'buy_put_transactions', 'Sell Puts Volume': 'put_sell_vol', 'Buy Puts Avg Price': 'bought_put_avg_price', 'Neutral Puts Volume': 'put_neutral_vol', 'Unknown Puts Transactions': 'unknown_put_transactions', 'Buy Puts Volume': 'put_buy_vol', 'Neutral Calls Volume': 'call_neutral_vol'})

  
        return df_summary
    


    async def target_all_strikes(self, ticker: str, headers):
        """
        Gather trade analysis for *all* strikes available for the given ticker.
        Returns a Pandas DataFrame where each row summarizes trade data for
        one strike (calls/puts combined) with columns for buy/sell/neutral/etc.
        """

        # 1) Fetch the options data for the ticker
        x = await self.multi_options(ticker=ticker, headers=headers)
        df = x.as_dataframe

        # Ensure we have the columns we need
        required_cols = {'strike', 'expiry', 'option_id', 'call_put'}
        if not required_cols.issubset(df.columns):
            print(f"Error: Missing required columns in dataframe for {ticker}. Found: {df.columns.tolist()}")
            return None

        if df.empty:
            print(f"No option data found for {ticker}.")
            return None

        # 2) Build a mapping from strike -> { 'calls': [option_ids], 'puts': [option_ids] }
        #    This way, we can fetch trade data for *all* option IDs concurrently.
        strike_map = {}
        for idx, row in df.iterrows():
            strike_value = float(row['strike'])
            call_or_put = row['call_put'].lower()  # 'call' or 'put'
            option_id = str(row['option_id'])

            if strike_value not in strike_map:
                strike_map[strike_value] = {'calls': [], 'puts': []}

            if call_or_put == 'call':
                strike_map[strike_value]['calls'].append(option_id)
            else:
                strike_map[strike_value]['puts'].append(option_id)

        # 3) We'll fetch trade data for all option IDs in *one* concurrency batch.
        #    First, gather *every* option ID from all strikes.
        all_option_ids = []
        # We'll also keep track of which strike each option ID belongs to,
        # so we can group them again after we fetch.
        option_id_to_strike = {}
        option_id_to_type = {}  # 'call' or 'put'

        for strike_value, group in strike_map.items():
            for opt_id in group['calls']:
                all_option_ids.append(opt_id)
                option_id_to_strike[opt_id] = strike_value
                option_id_to_type[opt_id] = 'call'
            for opt_id in group['puts']:
                all_option_ids.append(opt_id)
                option_id_to_strike[opt_id] = strike_value
                option_id_to_type[opt_id] = 'put'

        all_option_ids = list(set(all_option_ids))  # unique IDs

        async def fetch_option_data(session, option_id):
            """Fetches option trade data for a single option_id asynchronously."""
            url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryDeals?count=1000&tickerId={option_id}"
            async with session.get(url) as resp:
                # If rate-limit or errors, you might add try/except
                return await resp.json()

        # 4) Perform concurrency fetch of all option IDs
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [fetch_option_data(session, opt_id) for opt_id in all_option_ids]
            results = await asyncio.gather(*tasks)

        # 5) We now have a list of trade-data results, each possibly containing 
        #    "datas": [ { deal, volume, tradeBsFlag, ... }, ... ]
        #    We'll group them by strike -> call/put, then compute the summary.

        # Initialize data storage: strike -> { 'calls': [], 'puts': [] }
        trades_by_strike = {}
        for strike_value in strike_map.keys():
            trades_by_strike[strike_value] = {
                'calls': [],
                'puts': []
            }

        # Loop over each result alongside the corresponding option_id
        for option_id, data_json in zip(all_option_ids, results):
            strike_value = option_id_to_strike[option_id]
            call_or_put = option_id_to_type[option_id]  # 'call'/'put'

            # In some cases, data_json might not be dict, or might not have 'datas'
            if isinstance(data_json, dict):
                trade_list = data_json.get('datas', [])
                # Append these trades to the appropriate bucket
                trades_by_strike[strike_value][f"{call_or_put}s"].extend(trade_list)

        # 6) For each strike, we compute the same summary logic you used:
        #    "Buy Calls", "Sell Calls", "Neutral Calls", "Unknown Calls",
        #    "Buy Puts", etc. We'll store the final row into a list of dicts.
        summary_rows = []

        # We'll define a helper function to process trades (same as your code).
        def process_trade_data(data_list, suffix, trade_data_store):
            """
            Processes a list of trades (data_list) and updates trade_data_store 
            which is the local dictionary for that strike.
            """
            for i in data_list:
                price = i.get('deal')
                volume = i.get('volume')
                flag = i.get('tradeBsFlag')

                try:
                    price = float(price) if price is not None and price != "" else None
                    volume = float(volume) if volume is not None and volume != "" else None
                except ValueError:
                    continue

                if price is None or volume is None:
                    continue

                if flag == "B":
                    trade_type = "Buy"
                elif flag == "S":
                    trade_type = "Sell"
                elif flag == "N":
                    trade_type = "Neutral"
                else:
                    trade_type = "Unknown"

                trade_key = f"{trade_type} {suffix}"  # e.g. "Buy Calls"

                trade_data_store[trade_key]["total_volume"] += volume
                trade_data_store[trade_key]["total_value"] += price * volume
                trade_data_store[trade_key]["transaction_count"] += 1

        # Now, iterate over each strike and compute the final summary row
        for strike_value, data_dict in trades_by_strike.items():
            # Initialize your trade_data dictionary
            trade_data = {
                f"{t} Calls": {"total_volume": 0, "total_value": 0, "transaction_count": 0} for t in ["Buy", "Sell", "Neutral", "Unknown"]
            }
            trade_data.update({
                f"{t} Puts": {"total_volume": 0, "total_value": 0, "transaction_count": 0} for t in ["Buy", "Sell", "Neutral", "Unknown"]
            })

            # Process calls
            call_datas = data_dict['calls']
            process_trade_data(call_datas, "Calls", trade_data)

            # Process puts
            put_datas = data_dict['puts']
            process_trade_data(put_datas, "Puts", trade_data)

            # Build summary dict for this strike
            summary_dict = {"strike": strike_value}  # keep track of which strike

            # Convert from your trade_data to final columns
            for trade_type, metrics in trade_data.items():
                total_volume = metrics["total_volume"]
                total_value = metrics["total_value"]
                avg_price = total_value / total_volume if total_volume > 0 else 0.0

                # e.g. trade_type = "Buy Calls"
                # We'll create columns like "Buy Calls Volume", "Buy Calls Avg Price", "Buy Calls Transactions"
                vol_col = f"{trade_type} Volume"
                avg_col = f"{trade_type} Avg Price"
                tx_col = f"{trade_type} Transactions"

                summary_dict[vol_col] = total_volume
                summary_dict[avg_col] = round(avg_price, 2)
                summary_dict[tx_col] = metrics["transaction_count"]

            summary_rows.append(summary_dict)

        # 7) Convert summary_rows to a DataFrame
        if not summary_rows:
            print(f"No trade data found for ticker {ticker}.")
            return None

        df_summary = pd.DataFrame(summary_rows)

        # 8) You can rename columns if desired, similar to your logic. 
        #    Just adapt them to the new columns you have.
        #    For brevity, let's not rename *all* possible permutations here,
        #    but you can do so if you want consistent naming.
        #
        # Example partial rename:
        rename_dict = {
            'Buy Calls Volume': 'call_buy_vol',
            'Buy Calls Avg Price': 'bought_calls_avg_price',
            'Buy Calls Transactions': 'bought_call_transactions',
            'Sell Calls Volume': 'call_sell_vol',
            'Sell Calls Avg Price': 'sold_call_avg_price',
            'Sell Calls Transactions': 'sold_call_transactions',
            'Neutral Calls Volume': 'call_neutral_vol',
            'Neutral Calls Avg Price': 'neutral_call_avg_price',
            'Neutral Calls Transactions': 'neutral_call_transactions',
            'Unknown Calls Volume': 'call_unknown_vol',
            'Unknown Calls Avg Price': 'unknown_call_avg_price',
            'Unknown Calls Transactions': 'unknown_call_transactions',
            # same for Puts...
            'Buy Puts Volume': 'put_buy_vol',
            'Buy Puts Avg Price': 'bought_put_avg_price',
            'Buy Puts Transactions': 'bought_put_transactions',
            'Sell Puts Volume': 'put_sell_vol',
            'Sell Puts Avg Price': 'sold_put_avg_price',
            'Sell Puts Transactions': 'sold_put_transactions',
            'Neutral Puts Volume': 'put_neutral_vol',
            'Neutral Puts Avg Price': 'neutral_put_avg_price',
            'Neutral Puts Transactions': 'neutral_put_transactions',
            'Unknown Puts Volume': 'put_unknown_vol',
            'Unknown Puts Avg Price': 'unknown_put_avg_price',
            'Unknown Puts Transactions': 'unknown_put_transactions',
        }
        df_summary = df_summary.rename(columns=rename_dict)

        # Return the final DataFrame summarizing all strikes
        return df_summary
    
    
    async def oi_summary(self, ticker:str, headers):
        """Gets Open Interest (OI) and OI Change per expiry for a ticker."""
        ticker=ticker.upper()

        x = await self.multi_options(ticker=ticker, headers=headers)
        df = x.as_dataframe
        df['ticker'] = ticker

        # Split into Calls and Puts
        calls_df = df[df['call_put'] == 'call'].reset_index(drop=True)
        puts_df = df[df['call_put'] == 'put'].reset_index(drop=True)

        # Aggregate Open Interest (OI) and OI Change by Expiry
        call_summary = calls_df.groupby('expiry')[['oi', 'oi_change']].sum().reset_index()
        put_summary = puts_df.groupby('expiry')[['oi', 'oi_change']].sum().reset_index()

        # Rename columns for clarity
        call_summary.rename(columns={'oi': 'call_oi', 'oi_change': 'call_oi_change'}, inplace=True)
        put_summary.rename(columns={'oi': 'put_oi', 'oi_change': 'put_oi_change'}, inplace=True)

        # Merge both DataFrames on expiry date
        oi_summary = pd.merge(call_summary, put_summary, on='expiry', how='outer').fillna(0)


        return oi_summary
    

    async def volume_summary(self, ticker:str, headers):
        """Gets volume per strike for a ticker."""
        ticker = ticker.upper()
        x = await self.multi_options(ticker=ticker, headers=headers)
        df = x.as_dataframe
        df['ticker'] = ticker
        # Split into Calls and Puts
        calls_df = df[df['call_put'] == 'call'].reset_index(drop=True)
        puts_df = df[df['call_put'] == 'put'].reset_index(drop=True)

        # Tally Call/Put Volume by Expiry
        call_volume_by_expiry = calls_df.groupby('expiry')['volume'].sum().reset_index()
        put_volume_by_expiry = puts_df.groupby('expiry')['volume'].sum().reset_index()

        # Rename columns for clarity
        call_volume_by_expiry.rename(columns={'volume': 'call_volume'}, inplace=True)
        put_volume_by_expiry.rename(columns={'volume': 'put_volume'}, inplace=True)

        # Merge both DataFrames on expiration date
        volume_summary = pd.merge(call_volume_by_expiry, put_volume_by_expiry, on='expiry', how='outer').fillna(0)

        
        return volume_summary