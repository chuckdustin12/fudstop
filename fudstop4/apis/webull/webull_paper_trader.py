import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
import httpx
from discord_webhook import AsyncDiscordWebhook
from typing import List
from .paper_models import WebullContractData
load_dotenv()

from pandas import json_normalize


import pandas as pd
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions(database='fudstop3')
import os
import hashlib
from .screener_models import ScreenerResults,OptionScreenerResults
import time
import uuid
import pickle
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv
load_dotenv()
import httpx
import asyncio
class PaperTrader:
    def __init__(self, headers):
        self.headers=headers

        self.db = db()
        #miscellaenous
                #sessions
        self.ticker_df = pd.read_csv('files/ticker_csv.csv')
        self.ticker_to_id_map = dict(zip(self.ticker_df['ticker'], self.ticker_df['id']))
        self._region_code = 6
        self.zone_var = 'dc_core_r001'
        self.timeout = 15
        self.db = db(host='localhost', user='chuck', database='fudstop3', password='fud', port=5432)
        self.device_id = "gldaboazf4y28thligawz4a7xamqu91g"
        self.account_id = 19645763
        self.most_active_tickers = ['SNOW', 'IBM', 'DKNG', 'SLV', 'NWL', 'SPXS', 'DIA', 'QCOM', 'CMG', 'WYNN', 'PENN', 'HLF', 'CCJ', 'WW', 'NEM', 'MOS', 'SRPT', 'MS', 'DPST', 'AG', 'PAA', 'PANW', 'XPEV', 'BHC', 'KSS', 'XLP', 'LLY', 'MDB', 'AZN', 'NVO', 'BOIL', 'ZM', 'HUT', 'VIX', 'PDD', 'SLB', 'PCG', 'DIS', 'TFC', 'SIRI', 'TDOC', 'CRSP', 'BSX', 'BITF', 'AAL', 'EOSE', 'RIVN', 'X', 'CCL', 'SOXS', 'NOVA', 'TMUS', 'HES', 'LI', 'NVAX', 'TSM', 'CNC', 'IAU', 'GDDY', 'CVX', 'TGT', 'MCD', 'GDXJ', 'AAPL', 'NKLA', 'EDR', 'NOK', 'SPWR', 'NKE', 'HYG', 'FSLR', 'SGEN', 'DNN', 'BAX', 'CRWD', 'OSTK', 'XLC', 'RIG', 'SEDG', 'SNDL', 'RSP', 'M', 'CD', 'UNG', 'LQD', 'TTD', 'AMGN', 'EQT', 'YINN', 'MULN', 'FTNT', 'WBD', 'MRNA', 'PTON', 'SCHW', 'ABNB', 'EW', 'PM', 'UCO', 'TXN', 'DLR', 'KHC', 'MMAT', 'QQQ', 'GOOGL', 'AEM', 'RTX', 'AVGO', 'RBLX', 'PAAS', 'UUP', 'OXY', 'SQ', 'PLUG', 'CLF', 'GOEV', 'BKLN', 'ALB', 'BALL', 'SMH', 'CVE', 'F', 'KRE', 'TWLO', 'ARCC', 'ARM', 'U', 'SOFI', 'SBUX', 'FXI', 'BMY', 'HSBC', 'EFA', 'SVXY', 'VALE', 'GOLD', 'MSFT', 'OIH', 'ARKK', 'AMD', 'AA', 'DXCM', 'ABT', 'WOLF', 'FDX', 'SOXL', 'MA', 'KWEB', 'BP', 'SNAP', 'NLY', 'KGC', 'URA', 'UVIX', 'KMI', 'ACB', 'NET', 'W', 'GRAB', 'LMT', 'EPD', 'FCX', 'STNE', 'NIO', 'SU', 'ET', 'CVS', 'ADBE', 'MXL', 'HOOD', 'FUBO', 'RIOT', 'CRM', 'TNA', 'DISH', 'XBI', 'VFS', 'GPS', 'NVDA', 'MGM', 'MRK', 'ABBV', 'LABU', 'BEKE', 'VRT', 'LVS', 'CPNG', 'BA', 'MTCH', 'PEP', 'EBAY', 'GDX', 'XLV', 'UBER', 'GOOG', 'COF', 'XLU', 'BILI', 'XLK', 'VXX', 'DVN', 'MSOS', 'KOLD', 'XOM', 'BKNG', 'SPY', 'RUT', 'CMCSA', 'STLA', 'NCLH', 'GRPN', 'ZION', 'UAL', 'GM', 'NDX', 'TQQQ', 'COIN', 'WBA', 'CLSK', 'NFLX', 'FREY', 'AFRM', 'NAT', 'EEM', 'IYR', 'KEY', 'OPEN', 'DM', 'TSLA', 'BXMT', 'T', 'TZA', 'BAC', 'MARA', 'UVXY', 'LOW', 'COST', 'HL', 'CHTR', 'TMF', 'ROKU', 'DOCU', 'PSEC', 'XHB', 'VMW', 'SABR', 'USB', 'DDOG', 'DB', 'V', 'NOW', 'XRT', 'SMCI', 'PFE', 'NYCB', 'BIDU', 'C', 'SPX', 'ETSY', 'EMB', 'SQQQ', 'CHPT', 'DASH', 'VZ', 'DNA', 'CL', 'ANET', 'WMT', 'MRO', 'WFC', 'MO', 'USO', 'ENVX', 'INTC', 'GEO', 'VFC', 'WE', 'MET', 'CHWY', 'PBR', 'KO', 'TH', 'QS', 'BTU', 'GLD', 'JD', 'XLY', 'KR', 'ASTS', 'WDC', 'HTZ', 'XLF', 'COP', 'PATH', 'SHEL', 'MXEF', 'SE', 'SPCE', 'UPS', 'RUN', 'DOW', 'ASHR', 'ONON', 'DAL', 'SPXL', 'SAVE', 'LUV', 'HD', 'JNJ', 'LYFT', 'UNH',  'NEE', 'STNG', 'SPXU', 'MMM', 'VNQ', 'IMGN', 'MSTR', 'AXP', 'TMO', 'XPO', 'FEZ', 'ENPH', 'AX', 'NVCR', 'GS', 'MRVL', 'ADM', 'GILD', 'IBB', 'PARA', 'PINS', 'JBLU', 'SNY', 'BITO', 'PYPL', 'FAS', 'GME', 'LAZR', 'URNM', 'BX', 'MPW', 'UPRO', 'HPQ', 'AMZN', 'SAVA', 'TLT', 'ON', 'CAT', 'VLO', 'AR', 'IDXX', 'SWN', 'META', 'BABA', 'ZS', 'EWZ', 'ORCL', 'XOP', 'TJX', 'XP', 'EL', 'HAL', 'IEF', 'XLI', 'UPST', 'Z', 'TELL', 'LRCX', 'DLTR', 'BYND', 'PACW', 'CVNA', 'GSAT', 'CSCO', 'NU', 'KVUE', 'JPM', 'LCID', 'TLRY', 'AGNC', 'CGC', 'XLE', 'VOD', 'TEVA', 'JETS', 'UEC',  'ZIM', 'ABR', 'IQ', 'AMC', 'ALLY', 'HE', 'OKTA', 'ACN', 'MU', 'FLEX', 'SHOP', 'PLTR', 'CLX', 'LUMN', 'WHR', 'PAGP', 'IWM', 'WPM', 'TTWO', 'AI', 'ALGN', 'SPOT', 'BTG', 'IONQ', 'GE', 'DG', 'AMAT', 'XSP', 'PG', 'LULU', 'DE', 'MDT', 'RCL', 'RDDT']
    async def get_webull_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        ticker_id = self.ticker_to_id_map.get(symbol)
        return ticker_id
    async def get_webull_ids(self, symbols):
        """Fetch ticker IDs for a list of symbols in one go."""
        return {symbol: self.ticker_to_id_map.get(symbol) for symbol in symbols}
    def to_decimal(self, value: Optional[str]) -> str:
        """
        Convert percentage string to decimal string if needed.
        """
        if value is not None and float(value) > 1:
            return str(float(value) / 100)
        return value


    async def get_account_id(self):
        new_acc_url = f"https://act.webullfintech.com/webull-paper-center/api/paper/v1/account/myaccounts?isInit=true&version=v1&supportAccountTypes=CASH%2CMARGIN_FUTURES"

        async with httpx.AsyncClient() as client:
            data = await client.get(new_acc_url, headers=self.headers)
            if data.status_code == 200:
                data = data.json()
                paper_id = data[0].get('id')
                print(paper_id)
                return paper_id

    async def get_option_id(self, ticker):
        
        try:
            if ticker == 'SPX':
                ticker = 'SPXW'
            df = await self.____(ticker)
            await self.db.batch_insert_dataframe(df, table_name='ids', unique_columns='option_id', )
            return df
        except Exception as e:
            print(e)

    async def update_option_ids(self):
        
        await self.db.connect()
        tasks = [self.get_option_id(i) for i in self.most_active_tickers]

        await asyncio.gather(*tasks)
        await self.db.disconnect()


    async def ____(self, ticker):
        """TABLE NAME = ids"""
      
        try:
            ticker_id = await self.get_webull_id(ticker)
            payload = {"expireCycle":[3,2,4],"type":0,"quoteMultiplier":100,"count":-1,"direction":"all", 'tickerId': ticker_id}
            url = f"https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"
            async with httpx.AsyncClient(headers=self.headers) as client:
                data = await client.post(url, json=payload)

                if data.status_code == 200:
                    data = data.json()
                    expireDateList = data['expireDateList']
                    data = [i.get('data') for i in expireDateList]
                    flat_data = [item for sublist in data for item in sublist]
                    option_ids = [i.get('tickerId') for i in flat_data]
                    strike = [float(i.get('strikePrice')) for i in flat_data]
                    call_put = [i.get('direction') for i in flat_data]
                    expiry = [i.get('expireDate') for i in flat_data]
                    dict = { 
                        'option_id': option_ids,
                        'ticker': ticker,
                        'strike': strike,
                        'call_put': call_put,
                        'expiry': expiry,
                        
                    }
                    df = pd.DataFrame(dict)
                    return df
        except Exception as e:
            print(e)


    async def reset_account(self, amount:str='4000'):
        account_id = await self.get_account_id()
        url = f"https://act.webullfintech.com/webull-paper-center/api/paper/1/acc/reset/{account_id}/{amount}"
        async with httpx.AsyncClient() as client:
            data = await client.get(url, headers=self.headers)
            print(data.text)
            print(data)

    async def get_close_price(self, option_id:str='1044771278'):
        try:
            url=f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={option_id}"
            async with httpx.AsyncClient() as client:
                data = await client.get(url, headers=self.headers)
                if data.status_code == 200:
                    data = data.json()
                    close = [i.get('close') for i in data]

                    return close
        except Exception as e:
            print(e)

            
    async def option_trade(self, quantity:int=1, action:str='BUY', option_id:int=1044771278):
        try:
            price = await self.get_close_price(option_id)
            price = float(price[0])
            print(price)
            url = f"https://act.webullfintech.com/webull-paper-center/api/paper/v1/order/optionPlace"
            payload = {"accountId":self.account_id,"orderType":"MKT","timeInForce":"GTC","quantity":quantity,"action":action,"tickerId":option_id,"lmtPrice":price,"paperId":1,"orders":[{"action":action,"quantity":quantity,"tickerId":option_id,"tickerType":"OPTION"}],"tickerType":"OPTION","optionStrategy":"Single","serialId": str(uuid.uuid4()) }

            async with httpx.AsyncClient() as client:
                data = await client.post(url, headers=self.headers, json=payload)
                data = data.json()
                print(data)
                return data

        except Exception as e:
            print(e)


    async def get_positions(self):
        async with httpx.AsyncClient(headers=self.headers) as client:
           
            url=f"https://act.webullfintech.com/webull-paper-center/api/paper/v1/account/summary?paperId=1&accountId=19645763"
            data = await client.get(url)
            data = data.json()
            

    
            # # Flatten capital data
            # capital_data = data['capital'].copy()
            # netLiquidationValue = capital_data.get('netLiquidationValue') #scalar
            # unrealizedProfitLoss = capital_data.get('unrealizedProfitLoss') #scalar
            # unrealizedProfitLossRate = capital_data.get('unrealizedProfitLossRate') #scalar
            # buyingPower = capital_data.get('buyingPower') #scalar
            # totalCashValue = capital_data.get('totalCashValue') #scalar
            # totalMarketValue = capital_data.get('totalMarketValue') #scalar
            # totalCost = capital_data.get('totalCost') #scalar

            # positions_data = data['positions'].copy()
            # id = [i.get('id') for i in positions_data]
            # tickerType = [i.get('tickerType') for i in positions_data]
            # optionStrategy = [i.get('optionStrategy') for i in positions_data]
            # currency = [i.get('currency') for i in positions_data]
            # quantity = [i.get('quantity') for i in positions_data]
            # marketValue = [i.get('marketValue') for i in positions_data]
            # unrealizedProfitLoss = [i.get('unrealizedProfitLoss') for i in positions_data]
            # optionExercisePrice = [i.get('optionExercisePrice') for i in positions_data]
            # unrealizedProfitLossRate = [i.get('unrealizedProfitLossRate') for i in positions_data]
            # costPrice = [i.get('costPrice') for i in positions_data]
            # lastPrice = [i.get('lastPrice') for i in positions_data]
            # proportion = [i.get('proportion') for i in positions_data]
            # symbol = [i.get('symbol') for i in positions_data]
            # tickerSubType = [i.get('tickerSubType') for i in positions_data]


            # items = [i.get('items') for i in positions_data]# Step 1: Flatten capital data (excluding positions)
            capital_data = data['capital'].copy()
            positions_data = data['positions'].copy()

            # Step 2: Extract fields for positions and items
            rows = []  # To hold final flattened data

            for position in positions_data:
                items = position.pop('items')  # Extract the nested 'items' list within each position
                for item in items:
                    # Merge parent 'position' data with each nested 'item' data
                    combined_row = {**position, **item}
                    rows.append(combined_row)

            # Step 3: Create a DataFrame
            df = pd.DataFrame(rows)
            _ = df[['symbol', 'optionExercisePrice', 'optionType', 'optionExpireDate', 'lastPrice']]

            _ = _.rename(columns={'symbol': 'ticker', 'optionExercisePrice': 'strike', 'optionType': 'call_put', 'optionExpireDate': 'expiry', 'tickerId': 'option_id', 'lastPrice': 'price'})

            return _
            

    async def trade_query(self, seen_records, db):

        try:
            # Step 1: Fetch trade options from the database
            query = """
            SELECT ticker, strike, call_put, expiry, bid, ask from master_all_two where moneyness = 'OTM' and theta > -0.03 and spread_pct < 7 and underlying_price > 10 and vwap > close and vwap > low and vwap > open and call_bid < put_bid and macd_hour = 'bearish' order by speed desc;"""
            results = await db.fetch(query)
            df = pd.DataFrame(results, columns=['sym', 'strk', 'cp', 'exp', 'b', 'a'])



            # Replace "call" with "c" and "put" with "p"
            df['cp'] = df['cp'].replace({'call': 'call', 'put': 'put'})

            # Convert results to a set of current tickers
            current_tickers = set(df['sym'])

            # Determine new tickers
            df['status'] = df['sym'].apply(lambda x: 'New' if x not in seen_records else 'Exists')

            # Update the seen_records with the current tickers
            seen_records.update(current_tickers)

            # Filter new records
            new_records = df[df['status'] == 'New']

            tickers = new_records['sym'].tolist()
            strikes = new_records['strk'].tolist()
            call_puts = new_records['cp'].tolist()
            expiries = new_records['exp'].tolist()
            bids = new_records['b'].tolist()
            asks = new_records['a'].tolist()

            # Step 2: Buy all options first
            option_ids = []
            cost_basis_list = []

            for ticker, strike, call_put, expiry, bid, ask in zip(tickers, strikes, call_puts, expiries, bids, asks):
                # Query to get option_id
                query = f"""SELECT option_id FROM ids WHERE ticker = '{ticker}' AND strike = {strike} AND call_put = '{call_put}' AND expiry = '{expiry}'"""
                print(query)
                results = await db.fetch(query)
                df = pd.DataFrame(results, columns=['option_id'])
                option_id = df['option_id'].to_list()[0]

                # Execute BUY trade for each option
                trade = await self.option_trade(quantity=1, action='BUY', option_id=option_id)

                # Send Discord notification after each BUY
                hook = AsyncDiscordWebhook("https://discord.com/api/webhooks/1207045858105888808/kkXrxa9HwL8DEvnIFoWdcw1W7arOWs0zGn9Ss5MqhUPTm8S_qRL1otXXttxap15mvhe3", content=f"TRADING <#1285988500159598612>\n-{trade} | {ticker} | ${strike} | {call_put} | {expiry}\n- Bid: ${bid}\n- Ask: ${ask}\n\n*SELL TARGET: 5% stop.*")
                await hook.execute()

                # Store option_id and the ask price (as cost basis)
                option_ids.append(option_id)
                cost_basis_list.append(ask)

            # Step 3: Monitor prices for each option in a separate loop (outside the buy loop)
            while option_ids:
                for i, option_id in enumerate(option_ids):
                    ticker, strike, call_put, expiry = tickers[i], strikes[i], call_puts[i], expiries[i]
                    cost_basis = float(cost_basis_list[i])

                    # Get the current market price of the option
                    check_price = await self.get_close_price(option_id)

                    # Set thresholds for selling
                    upper_limit = cost_basis * 1.50  # 5% above cost basis
                    lower_limit = cost_basis * 0.75  # 5% below cost basis

                    if float(check_price[0]) >= upper_limit:
                        # Execute SELL trade (profitable exit)
                        sell_trade = await self.option_trade(quantity=1, action='SELL', option_id=option_id)
                        content = f"SOLD {ticker} | ${strike} | {call_put} | {expiry}\n- Price: ${check_price}\n- Result: PROFIT"
                        hook = AsyncDiscordWebhook("https://discord.com/api/webhooks/1207045858105888808/kkXrxa9HwL8DEvnIFoWdcw1W7arOWs0zGn9Ss5MqhUPTm8S_qRL1otXXttxap15mvhe3", content=content)
                        await hook.execute()

                        # Remove the option from monitoring list
                        option_ids.pop(i)
                        cost_basis_list.pop(i)

                    elif float(check_price[0]) <= lower_limit:
                        # Execute SELL trade (stop loss exit)
                        sell_trade = await self.option_trade(quantity=1, action='SELL', option_id=option_id)
                        print(f"Selling {ticker} at loss: {sell_trade}")
                        content = f"SOLD {ticker} | ${strike} | {call_put} | {expiry}\n- Price: ${check_price}\n- RESULT: LOSS"
                        hook = AsyncDiscordWebhook("https://discord.com/api/webhooks/1207045858105888808/kkXrxa9HwL8DEvnIFoWdcw1W7arOWs0zGn9Ss5MqhUPTm8S_qRL1otXXttxap15mvhe3", content=content)
                        await hook.execute()

                        # Remove the option from monitoring list
                        option_ids.pop(i)
                        cost_basis_list.pop(i)

                # Sleep before checking again
                await asyncio.sleep(5)  # Check price every 5 seconds or adjust the interval

        except Exception as e:
            print(e)
