import os
import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from dotenv import load_dotenv
load_dotenv()
import aiohttp
import pandas as pd
from pytz import timezone

from .trade_models.capital_flow import CapitalFlow
from .trade_models.cost_distribution import CostDistribution
from .trade_models.etf_holdings import ETFHoldings
from .trade_models.institutional_holdings import InstitutionHolding, InstitutionStat
from .trade_models.financials import BalanceSheet, FinancialStatement, CashFlow
from .trade_models.news import NewsItem
from .trade_models.forecast_evaluator import ForecastEvaluator
from .trade_models.short_interest import ShortInterest
from .trade_models.volume_analysis import WebullVolAnalysis
from .trade_models.ticker_query import WebullStockData, MultiQuote
from .trade_models.analyst_ratings import Analysis


from datetime import datetime, timedelta

class WebullTrading:
    def __init__(self):


        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.timeframes = ['m1','m5', 'm10', 'm15', 'm20', 'm30', 'm60', 'm120', 'm240', 'd1']

        self.headers = {
        "Access_token": os.environ.get('ACCESS_TOKEN'),
        "App": "global",
        "App-Group": "broker",
        "Appid": "wb_web_app",
        "Content-Type": "application/json",
        "Device-Type": "Web",
        "Did": os.environ.get('DID'),
        "Hl": "en",
        "Locale": "eng",
        "Os": "web",
        "Osv": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ph": "Windows Chrome",
        "Platform": "web",
        "Referer": "https://app.webull.com/",
        "Reqid": os.environ.get('REQ_ID'),
        "Sec-Ch-Ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "T_time": "1698276695206",
        "Tz": "America/Los_Angeles",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ver": "3.40.11",
        "X-S": "49ef20ad66d1e24a83ff8b2015bc13c6d133285c5665dbbe4aa6032572749931",
        "X-Sv": "xodp2vg9"
    }
    async def fetch_endpoint(self, endpoint):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(endpoint) as resp:
                return await resp.json()

    async def get_ticker_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        endpoint =f"https://quotes-gw.webullfintech.com/api/search/pc/tickers?keyword={symbol}&pageIndex=1&pageSize=1"

        
        data =  await self.fetch_endpoint(endpoint)
        datas = data['data'] if 'data' in data else None
        if datas is not None:
            tickerID = datas[0]['tickerId']
            return tickerID
    

    async def get_bars(self, symbol, timeframe:str='m1'):
        """
        Timeframes:
        
        >>> m1: 1 minute
        >>> m5: 5 minute
        >>> m10: 10 minute
        >>> m15: 15 minute
        >>> m20: 20 minute
        >>> m30: 30 minute
        >>> m60: 1 hour
        >>> m120: 2 hour
        >>> m240: 4 hour
        
        """
        tickerid = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/quote/charts/query?tickerIds={tickerid}&type={timeframe}&count=800&extendTrading=0"
        datas =  await self.fetch_endpoint(endpoint)
        if datas is not None:
            data = datas[0]['data']
            # Create empty lists for each column
            timestamps = []
            column2 = []
            column3 = []
            column4 = []
            column5 = []
            column6 = []

            # Split each line and append values to respective lists
            for line in data:
                parts = line.split(',')
                timestamps.append(parts[0])
                column2.append(parts[1])
                column3.append(parts[2])
                column4.append(parts[3])
                column5.append(parts[4])
                column6.append(parts[5])

  

            df = pd.DataFrame({
                'Timestamp': timestamps,
                'Open': column2,
                'Low': column3,
                'High': column4,
                'Close': column5,
                'Vwap': column6
            })

            # Convert the 'Timestamp' column to integers before converting to datetime
            df['Timestamp'] = df['Timestamp'].astype(int)

            # Then convert to datetime
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')


            # Convert to Eastern Time
            eastern = timezone('US/Eastern')
            df['Timestamp'] = df['Timestamp'].dt.tz_localize('UTC').dt.tz_convert(eastern)

            # Remove the timezone information
            df['Timestamp'] = df['Timestamp'].dt.tz_localize(None)
            df['Timeframe'] = timeframe
            df['Ticker'] = symbol
  
            return df
        


    async def get_stock_quote(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)

        endpoint = f"https://quotes-gw.webullfintech.com/api/stock/tickerRealTime/getQuote?tickerId={ticker_id}&includeSecu=1&includeQuote=1&more=1"
        datas = await self.fetch_endpoint(endpoint)

        data = WebullStockData(datas)
        return data


    async def get_analyst_ratings(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint=f"https://quotes-gw.webullfintech.com/api/information/securities/analysis?tickerId={ticker_id}"
        datas = await self.fetch_endpoint(endpoint)
        data = Analysis(datas)
        return data
    

    async def get_short_interest(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/brief/shortInterest?tickerId={ticker_id}"
        datas = await self.fetch_endpoint(endpoint)
        data = ShortInterest(datas)
        return data
    
    async def institutional_holding(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/stock/getInstitutionalHolding?tickerId={ticker_id}"
        datas = await self.fetch_endpoint(endpoint)
        data = InstitutionStat(datas)

        return data
    

    async def volume_analysis(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count=10&tickerId={ticker_id}&type=0"
        datas = await self.fetch_endpoint(endpoint)
        data = WebullVolAnalysis(datas)
        return data
    

    async def cost_distribution(self, symbol:str, start_date:str=None, end_date:str=None):

        if start_date is None:
            start_date = self.yesterday
            

        if end_date is None:
            end_date = self.today

        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/quotes/chip/query?tickerId={ticker_id}&startDate={start_date}&endDate={end_date}"
 
        datas = await self.fetch_endpoint(endpoint)
        data = CostDistribution(datas)
        return data
    

    async def stock_quote(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_id}&includeSecu=1&delay=0&more=1"
        datas = await self.fetch_endpoint(endpoint)
        data = WebullStockData(datas)
        return data
    

    async def news(self, symbol:str, pageSize:str='100'):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://nacomm.webullfintech.com/api/information/news/tickerNews?tickerId={ticker_id}&currentNewsId=0&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        data = NewsItem(datas)
        return data
    

    async def balance_sheet(self, symbol:str, limit:str='11'):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/balancesheet?tickerId={ticker_id}&type=101&fiscalPeriod=0&limit={limit}"
        datas = await self.fetch_endpoint(endpoint)
        data = BalanceSheet(datas)
        return data
    
    async def cash_flow(self, symbol:str, limit:str='12'):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/cashflow?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
        datas = await self.fetch_endpoint(endpoint)
        data = CashFlow(datas)
        return data
    
    async def income_statement(self, symbol:str, limit:str='12'):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/incomestatement?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
        datas = await self.fetch_endpoint(endpoint)
        data = FinancialStatement(datas)
        return data
    



    async def capital_flow(self, symbol:str):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/ticker?tickerId={ticker_id}&showHis=true"
        datas = await self.fetch_endpoint(endpoint)
        data = CapitalFlow(datas)
        return data
    

    async def etf_holdings(self, symbol:str, pageSize:str='200'):
        ticker_id = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/company/queryEtfList?tickerId={ticker_id}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        data = ETFHoldings(datas)
        return data
    

    async def multi_quote(self, symbols:str):
        counter = 0
        while True:
            counter = counter + 1
           
            ticker_ids = [await self.get_ticker_id(i) for i in symbols]
            ticker_ids = str(ticker_ids)
            ticker_ids = ','.join([ticker_ids]).replace(']', '').replace('[', '').replace(' ', '')
            endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_ids}&includeSecu=1&delay=0&more=1"

            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.get(endpoint) as resp:
                    datas = await resp.json()
                    all_data = MultiQuote(datas)

                    for sym,price, vol, vr in zip(all_data.symbol, all_data.close, all_data.volume, all_data.vibrateRatio):
                        yield(f'SYM(1): | {sym} | PRICE(3): | {price} | VOL:(5): | {vol} | VIBRATION:(7): | {vr}')

                        if counter == 250:
                            print(f"Stream ending...")
                        
                            break