import os
from dotenv import load_dotenv
load_dotenv()
import httpx
import asyncio
from .trader_models.trader_models import Capital, DT_DAY_DETAIL_LIST, Positions, OpenPositions, OrderHistory
from polygonio.polygon_database import PolygonDatabase
from typing import Optional
import pandas as pd
account_id = os.environ.get('webull_account_id')
from datetime import datetime, timedelta

from .trader_models.trader_models import OptionData

class PaperTrader:
    def __init__(self):
        self.id = 15765933
        self.headers  = {
        "Accept": os.getenv("ACCEPT"),
        "Accept-Encoding": os.getenv("ACCEPT_ENCODING"),
        "Accept-Language": "en-US,en;q=0.9",
        'Content-Type': 'application/json',
        "App": os.getenv("APP"),
        "App-Group": os.getenv("APP_GROUP"),
        "Appid": os.getenv("APPID"),
        "Device-Type": os.getenv("DEVICE_TYPE"),
        "Did": 'gldaboazf4y28thligawz4a7xamqu91g',
        "Hl": os.getenv("HL"),
        "Locale": os.getenv("LOCALE"),
        "Origin": os.getenv("ORIGIN"),
        "Os": os.getenv("OS"),
        "Osv": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Ph": os.getenv("PH"),
        "Platform": os.getenv("PLATFORM"),
        "Priority": os.getenv("PRIORITY"),
        "Referer": os.getenv("REFERER"),
        "Reqid": os.getenv("REQID"),
        "T_time": os.getenv("T_TIME"),
        "Tz": os.getenv("TZ"),
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Ver": os.getenv("VER"),
        "X-S": os.getenv("X_S"),
        "X-Sv": os.getenv("X_SV")
    }
        #miscellaenous
                #sessions
        self._account_id = ''
        self._trade_token = ''
        self._access_token = ''
        self._refresh_token = ''
        self._token_expire = ''
        self._uuid = ''

        self._region_code = 6
        self.zone_var = 'dc_core_r001'
        self.timeout = 15
        self.db = PolygonDatabase(host='localhost', user='chuck', database='fudstop2', password='fud', port=5432)
        self.device_id = "gldaboazf4y28thligawz4a7xamqu91g"

    def to_decimal(self, value: Optional[str]) -> str:
        """
        Convert percentage string to decimal string if needed.
        """
        if value is not None and float(value) > 1:
            return str(float(value) / 100)
        return value


    async def get_token(self):
        endpoint = f"https://u1suser.webullfintech.com/api/user/v1/login/account/v2"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.post(endpoint, json={"account":"brainfartastic@gmail.com","accountType":"2","pwd":"306a2ecebccfb37988766fac58f9d0e3","deviceId":"gldaboazf4y28thligawz4a7xamqu91g","deviceName":"Windows Chrome","grade":1,"regionId":1})
            data = data.json()
            token = data.get('accessToken')
            return token



  

    async def update_trade_token(self):
        payload = { 'pwd': '5ad14adc3d09d9517fecfb031e3676e9'}
        endpoint = f"https://u1suser.webullfintech.com/api/user/v1/security/login"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.post(endpoint, json=payload, headers=self.headers)
            data = data.json()
            token = data.get('tradeToken')
            
            return token
    

    async def get_account_detail(self, account_id:str=account_id):
        """Gets trading summary."""
        token = await self.update_trade_token()
        self.headers.update({"T_Token": token})
        endpoint = f"https://ustrade.webullfinance.com/api/trading/v1/webull/asset/summary?secAccountId={account_id}"

        async with httpx.AsyncClient(headers=self.headers) as client:
            data = await client.get(endpoint, headers=self.headers)
            data = data.json()
            print(data)
            capital = data['capital']

            return Capital(capital)
        

    async def profit_loss(self):
        endpoint=f"https://ustrade.webullfinance.com/api/trading/v1/webull/profitloss/account/getProfitlossAccountSummary?secAccountId=12165004&startDate=2024-04-19&endDate=2024-04-23"
        token = await self.update_trade_token()
        self.headers.update({"T_Token": token})
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint, headers=self.headers)
            data = data.json()

            return data


    async def get_option_data(self, option_ids, headers):
        endpoint = f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={option_ids}"
        token = await self.update_trade_token()
        self.headers.update({"T_Token": token})
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint, headers=headers)
            data = data.json()

            return OptionData(data)


    async def positions(self):
        """RETURNS OPEN POSITIONS AND ACCOMPANYING DATA"""
        endpoint = f"https://ustrade.webullfinance.com/api/trading/v1/webull/asset/summary?secAccountId={account_id}"
        token = await self.update_trade_token()
        self.headers.update({"T_Token": token})
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint, headers=self.headers)
            data = data.json()

            pos = data['positions']
            items = [i.get('items') for i in pos]
            items = [item for sublist in items for item in sublist]

            positions = Positions(data['positions'])

            open_positions = OpenPositions(items)

            option_ids = open_positions.tickerId

            option_ids_str = ','.join(map(str, option_ids))

            option_data = f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={option_ids_str}"

            async with httpx.AsyncClient() as client:
                data = await client.get(option_data, headers=self.headers)
                data = data.json()
                return open_positions, OptionData(data)




    async def get_order_history(self):

        """GETS ACCOUNT ORDER HISTORY
        
        RETURNS A TUPLE
        """


        endpoint = f"https://ustrade.webullfinance.com/api/trading/v1/webull/order/list?secAccountId=12165004"
        payload ={"dateType":"ORDER","pageSize":1000,"startTimeStr":"2024-04-01","endTimeStr":"2024-04-27","action":None,"lastCreateTime0":0,"secAccountId":12165004,"status":"all"}
        token = await self.update_trade_token()
        self.headers.update({"T_Token": token})
        async with httpx.AsyncClient() as client:

            data = await client.post(endpoint, headers=self.headers, json=payload)

            data = data.json()


            history_data =  OrderHistory(data)

            ticker_ids = history_data.tickerId

            tasks = [self.get_option_data(i) for i in ticker_ids]

            results = await asyncio.gather(*tasks)

            return history_data, results[0]