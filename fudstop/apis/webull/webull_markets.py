import os
connection_string = os.environ.get('CONNECTION_STRING')
import aiohttp
import asyncio
import httpx
from .trade_models.econ_data import EconomicData, EconEvents
from .trade_models.treasury_models import TreasuryData
from .newmodels import EarningsSurprises,Alerts,Econ, TCSummaries
from .trade_models.us_treasuries import US_TREASURIES
import pandas as pd
from fudstop.apis.webull.webull_option_screener import WebullOptionScreener
from pytz import timezone
import matplotlib.pyplot as plt
from .industry import IndustryData
from .database_manager import DatabaseManager
from matplotlib.dates import DateFormatter
import pytz
from datetime import datetime
from .futures import WBFutures, IndividualFutures
from .toprank_models import EarningSurprise, Dividend, MicroFutures
from .newmodels import EarningsData
from .webull_helpers import parse_most_active, parse_total_top_options, parse_contract_top_options, parse_ticker_values, parse_ipo_data, parse_etfs
screen = WebullOptionScreener()
class WebullMarkets(DatabaseManager):
    """General market data from webull"""
    def __init__(self,host='localhost', user='chuck', database='market_data', password='fud', port=5432):
        self.micro_assets_dict = {
            "micro.MYM": "Micro E-mini Dow",
            "micro.MES": "Micro E-mini S&P 500",
            "micro.MNQ": "Micro E-mini Nasdaq-100",
            "micro.M2K": "Micro E-mini Russell 2000",
            "micro.M6A": "Micro AUD/USD",
            "micro.M6B": "Micro GBP/USD",
            "micro.MCD": "Micro CAD/USD",
            "micro.M6E": "Micro EUR/USD",
            "micro.MSF": "Micro CHF/USD",
            "micro.MGC": "Micro Gold",
            "micro.MHG": "Micro Copper",
            "micro.SIL": "Micro Silver",
            "micro.MCL": "Micro WTI Crude Oil",
            "micro.10Y": "Micro 10-Year Yield",
            "micro.30Y": "Micro 30-Year Yield",
            "micro.2YY": "Micro 2-Year Yield",
            "micro.5YY": "Micro 5-Year Yield",
            "micro.MBT": "Micro Bitcoin",
            "micro.MET": "Micro Ether"
        }
        self.today = datetime.today().strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.today() + pd.Timedelta(days=30)).strftime('%Y-%m-%d')
        self.interest_rate_dict = {
            "interestRate.ZT": "2-Year T-Note",
            "interestRate.ZF": "5-Year T-Note",
            "interestRate.ZN": "10-Year T-Note",
            "interestRate.TN": "Ultra 10-Year T-Note",
            "interestRate.10Y": "Micro 10-Year Yield",
            "interestRate.UB": "Ultra T-Bond",
            "interestRate.ZB": "U.S. Treasury Bond",
            "interestRate.30Y": "Micro 30-Year Yield",
            "interestRate.2YY": "Micro 2-Year Yield",
            "interestRate.5YY": "Micro 5-Year Yield"
        }

        self.agricultural_dict = {
            "agricultural.ZS": "Soybeans",
            "agricultural.ZW": "Chicago Wheat",
            "agricultural.ZC": "Corn",
            "agricultural.GF": "Feeder Cattle",
            "agricultural.HE": "Lean Hogs",
            "agricultural.LE": "Live Cattle",
            "agricultural.XC": "Mini-Corn",
            "agricultural.XK": "Mini Soybean",
            "agricultural.ZL": "Soybean Oil",
            "agricultural.ZM": "Soybean Meal",
            "agricultural.ZO": "Oats",
            "agricultural.XW": "Mini-sized Chicago Wheat"
        }
        self.crypto_dict =   {"cryptocurrency.BTC": "Bitcoin",
                        "cryptocurrency.MBT": "Micro Bitcoin",
                        "cryptocurrency.ETH": "Ether",
                        "cryptocurrency.MET": "Micro Ether"}
        self.energy_dict = {
            "energy.CL": "Crude Oil",
            "energy.NG": "Natural Gas (Henry Hub)",
            "energy.RB": "RBOB Gasoline",
            "energy.BZ": "Brent Crude Oil",
            "energy.QM": "E-mini Crude Oil",
            "energy.MCL": "Micro WTI Crude Oil",
            "energy.HO": "NY Harbor ULSD",
            "energy.QG": "E-mini Natural Gas"
        }
        self.metal_dict  = {
            "metal.GC": "Gold",
            "metal.SI": "Silver",
            "metal.PL": "Platinum",
            "metal.QO": "E-mini Gold",
            "metal.MGC": "Micro Gold",
            "metal.HG": "Copper",
            "metal.MHG": "Micro Copper",
            "metal.QC": "E-mini Copper",
            "metal.SIL": "Micro Silver",
            "metal.QI": "E-mini Silver",
            "metal.PA": "Palladium"
        }

        self.equity_index_dict = {
            "equityIndex.YM": "E-mini Dow",
            "equityIndex.NQ": "E-mini Nasdaq",
            "equityIndex.ES": "E-mini S&P",
            "equityIndex.RTY": "E-mini Russell 2000",
            "equityIndex.EMD": "E-mini S&P MidCap 400",
            "equityIndex.MES": "Micro E-mini S&P 500",
            "equityIndex.MNQ": "Micro E-mini Nasdaq-100",
            "equityIndex.M2K": "Micro E-mini Russell 2000",
            "equityIndex.MYM": "Micro E-mini Dow"
        }

        self.fx_dict  = {
            "fx.6E": "EUR/USD",
            "fx.6J": "JPY/USD",
            "fx.6B": "GBP/USD",
            "fx.6A": "AUD/USD",
            "fx.M6A": "Micro AUD/USD",
            "fx.M6B": "Micro GBP/USD",
            "fx.6C": "CAD/USD",
            "fx.MCD": "Micro CAD/USD",
            "fx.E7": "E-mini EUR/USD",
            "fx.M6E": "Micro EUR/USD",
            "fx.J7": "E-mini JPY/USD",
            "fx.6S": "CHF/USD",
            "fx.MSF": "Micro CHF/USD"
        }
        self.host=host
        self.user=user
        self.port=port
        self.password=password
        self.database=database

        self.most_active_types = ['rvol10d', 'turnoverRatio', 'volume', 'range']
        self.top_option_types = ['totalVolume', 'totalPosition', 'volume', 'position', 'impVol', 'turnover']
        self.top_gainer_loser_types = ['afterMarket', 'preMarket', '5min', '1d', '5d', '1m', '3m', '52w']
        self.etf_types = ['industry', 'index', 'commodity', 'other']
        self.high_and_low_types = ['newHigh', 'newLow', 'nearHigh', 'nearLow']



        self.headers = {
        "App": "global",
        'Access-Token': os.environ.get('ACCESS_TOKEN'),
        "App-Group": "broker",
        "Appid": "wb_web_app",
        "Device-Type": "Web",
        "Did": os.environ.get('DID'),
        "Hl": "en",
        "Locale": "eng",
        "Os": "web",
        "Osv": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ph": "Windows Chrome",
        "Platform": "web",
        "Referer": "https://app.webull.com/",
        "Sec-Ch-Ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "T_time": "1698276695206",
        "Tz": "America/Los_Angeles",

    }

    async def fetch_endpoint(self, endpoint):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(endpoint) as resp:
                return await resp.json()
            
    

    async def get_top_options(self, rank_type:str='volume', as_dataframe:bool = True):
        """Rank Types:
        
        >>> totalVolume (total contract volume for a ticker)
        >>> totalPosition (total open interest for a ticker)
        >>> volume (volume by contract)
        >>> position (open interest by contract)
        >>> posIncrease (open interest increase)
        >>> posDecrease (open interest decrease)
        >>> impVol 
        >>> turnover

        DEFAULT: volume"""
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/option/rank/list?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=350"
        datas = await self.fetch_endpoint(endpoint)
        data = datas['data']
        if 'total' in rank_type:
            total_data = await parse_total_top_options(data)
            df = pd.DataFrame(total_data)
            
      
        else:
            total_data = await parse_contract_top_options(data)
            df= pd.DataFrame(total_data)
            
    
        if as_dataframe == False:
            return total_data
        
        df['rank_type'] = rank_type

        df.columns = df.columns.str.lower()
        print(df.columns)
        df = df.drop(columns=['dt', 'sectype', 'fatradetime', 'tradetime', 'status', 'template'])
        df['totalasset'] = df['totalasset'].astype(float)
        df['netasset'] = df['netasset'].astype(float)
        if 'implvol' in df.columns:
            df['implvol'] = df['implvol'].astype(float)
        df['position'] = df['position'].astype(float)
        if 'middleprice' in df.columns:
            df['middleprice'] = df['middleprice'].astype(float)
        if 'turnover' in df.columns:
            df['turnover'] = df['turnover'].astype(float)

        if 'positionchange' in df.columns:
            df['positionchange'] = df['positionchange'].astype(float)
        if 'unsymbol' in df.columns:
            df['unsymbol'] = df['unsymbol'].astype('string')
        if 'strikeprice' in df.columns:
            df['strikeprice'] = df['strikeprice'].astype(float)
        if 'price' in df.columns:
            df['price'] = df['price'].astype(float)
        if 'direction' in df.columns:
            df['direction'] = df['direction'].astype('string')
        # Convert columns to float
        float_columns = ['close', 'change', 'changeratio', 'marketvalue', 'volume', 'turnoverrate',
                        'pettm', 'preclose', 'fiftytwowkhigh', 'fiftytwowklow', 'open', 'high', 
                        'low', 'vibrateratio', 'pchratio', 'pprice', 'pchange']
        df[float_columns] = df[float_columns].astype(float)

    
          
     

  
 
        await self.connect()


        
        return df


    async def get_all_futures(self, futures_type):
        """Get futures data for a given type"""
        if futures_type not in self.futures_categories:
            print(f"Futures type '{futures_type}' not found.")
            return

        async with httpx.AsyncClient() as client:
            tasks = []
            futures_dict = self.futures_categories[futures_type]
            for abbreviation in futures_dict.keys():
                endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?regionId=6&rankType={futures_type}.{abbreviation}&pageIndex=1&pageSize=50"
                print(endpoint)
                tasks.append(self.fetch_futures_data(client, endpoint, futures_type, abbreviation))

            results = await asyncio.gather(*tasks)

            data = [i.get('data') for i in results]
            data = [i.get('data') for i in data]
            data = [item for sublist in data for item in sublist]

            values = [i.get('values') for i in data]
            futures = [i.get('futures') for i in data]

            return WBFutures(values, futures)
        

    async def get_equity_index_futures(self, abbreviation):
        """Get futures data for a given type"""

        async with httpx.AsyncClient() as client:

            endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?regionId=6&rankType=equityIndex.{abbreviation}&pageIndex=1&pageSize=50"

            data = await client.get(endpoint)
            results = data.json()


            data = results['data']
            values = [i.get('values') for i in data]
            futures = [i.get('futures') for i in data]

            return WBFutures(values, futures)
        

    async def get_industry_data(self, level:int=2, direction:int=-1, timespan:str='5d', page_size:int=100,top_num:int=100):
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/industry/IndustryList"
        headers = self.headers
        payload = {"industryLevel":level,"direction":direction,"industryType":timespan,"pageIndex":1,"pageSize":page_size,"regionId":6,"topNum":top_num}
        async with httpx.AsyncClient() as client:
            data = await client.post(endpoint, json=payload, headers=headers)
            data = data.json()

            return IndustryData(data)

    async def get_es_main(self):

        """GET ES MAIN DATA"""
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids=470004426&includeSecu=1&includeQuote=1&more=1"
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)
            data = data.json()

            return IndividualFutures(data)

    async def fetch_futures_data(self, client, endpoint, futures_type, abbreviation):
        response = await client.get(endpoint)
        data = response.json()
        return { 'type': futures_type, 'abbreviation': abbreviation, 'data': data }

    async def get_most_active(self, rank_type:str='rvol10d', as_dataframe:bool=False):
        """Rank types: 
        
        >>> volume
        >>> range
        >>> turnoverRatio
        >>> rvol10d
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/topActive?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=350"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_most_active(datas)
        if as_dataframe == False:
            return parsed_data
        df = pd.DataFrame(parsed_data)
        df['rank_type'] = rank_type
        df.columns = df.columns.str.lower()

        await self.connect()

        os.makedirs(f'data/top_active', exist_ok=True)

        df.to_csv(f'data/top_active/top_active_{rank_type}.csv', index=False)
        return df


        
    async def get_all_rank_types(self,types):
        """
        types:

        >>> wb.most_active_types
        >>> wb.top_option_types
        """
        if types == self.top_option_types:
            tasks = [self.get_top_options(type) for type in types]
            results = await asyncio.gather(*tasks)

            return results
        elif types == self.most_active_types:
            tasks = [self.get_most_active(type) for type in types]
            results = await asyncio.gather(*tasks)
            return results           


    async def earnings(self, start_date:str, end_date:str, pageSize: str='100', as_dataframe:str=True):
        """
        Pulls a list of earnings.

        >>> Start Date: enter a start date in YYYY-MM-DD format.

        >>> pageSize: enter the amount to be returned. default = 100

        >>> as_dataframe: default returns as a pandas dataframe.
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/market/calendar/earnings?pageSize={pageSize}&startDate={start_date}&endDate={end_date}&pageIndex=1&regionIds=6&timePeriods=1%2C2%2C3%2C4&timeZone=America%2FNew_York"
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)
            data = data.json()
            
            return EarningsData(data)

    async def get_top_gainers(self, rank_type:str='1d', pageSize: str='100', as_dataframe:bool=True):
        """
        Rank Types:

        >>> afterMarket
        >>> preMarket
        >>> 5min
        >>> 1d (daily)
        >>> 5d (5day)
        >>> 1m (1month)
        >>> 3m (3month)
        >>> 52w (52 week)  

        DEFAULT: 1d (daily) 


        >>> PAGE SIZE:
            Number of results to return. Default = 100     
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/market/topGainers?regionId=6&rankType={rank_type}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_ticker_values(datas)
        if as_dataframe == False:
            return parsed_data
        df = pd.DataFrame(parsed_data)
        df['rank_type'] = rank_type
        df['gainer_type'] = 'topGainers'

        df.columns = df.columns.str.lower()

        if 't_sectype' in df.columns:
            df = df.drop(columns=['t_sectype'])
        await self.connect()
        return df
    

    async def get_top_losers(self, rank_type:str='1d', pageSize: str='100', as_dataframe:bool=True):
        """
        Rank Types:

        >>> afterMarket
        >>> preMarket
        >>> 5min
        >>> 1d (daily)
        >>> 5d (5day)
        >>> 1m (1month)
        >>> 3m (3month)
        >>> 52w (52 week)  

        DEFAULT: 1d (daily) 


        >>> PAGE SIZE:
            Number of results to return. Default = 100     
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/market/dropGainers?regionId=6&rankType={rank_type}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_ticker_values(datas)
        if as_dataframe == False:
            return parsed_data
        df = pd.DataFrame(parsed_data)
        df['rank_type'] = rank_type
        df['gainer_type'] = 'topLosers'


        df.columns = df.columns.str.lower()
        if 't_sectype' in df.columns:
            df.drop(['t_sectype'], axis=1, inplace=True)
        await self.connect()
        return df
    
    async def get_all_gainers_losers(self, type:str='gainers'):
        """TYPE OPTIONS:
        >>> gainers - all gainers across all rank_types
        >>> losers - all losers across all rank_types
        
        """
        types = self.top_gainer_loser_types
        if type == 'gainers':
            tasks = [self.get_top_gainers(type) for type in types]
            results = await asyncio.gather(*tasks)
            return results
        

        elif type == 'losers':
            tasks =[self.get_top_losers(type) for type in types]
            results = await asyncio.gather(*tasks)
            return results

    async def get_forex(self):
        endpoint = "https://quotes-gw.webullfintech.com/api/bgw/market/load-forex"
        datas = await self.fetch_endpoint(endpoint)

        df = pd.DataFrame(datas)


        df.columns = df.columns.str.lower()
        if 't_sectype' in df.columns:
            df.drop(['t_sectype'], axis=1, inplace=True)
        await self.connect()

        return df
    
    async def etf_finder(self, type:str='industry'):
        """
        TYPES:

        >>> index
        >>> industry
        >>> commodity
        >>> other
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/etfinder/pcFinder?topNum=5&finderId=wlas.etfinder.{type}&nbboLevel=true"
        datas = await self.fetch_endpoint(endpoint)
        data = await parse_etfs(datas)

        df = pd.DataFrame(data)
        df['type'] = type
 
        df.columns = df.columns.str.lower()
        df = df.drop(columns=['id', 'sectype', 'exchangetrade'])
        await self.connect()
  

        return df
    
    async def get_all_etfs(self, types):
        types = self.etf_types
        tasks =[self.etf_finder(type) for type in types]

        results = await asyncio.gather(*tasks)

        return results


    async def highs_and_lows(self, type:str='newLow', pageSize:str='200', as_dataframe:bool=True):
        """
        TYPES:

        >>> newLow
        >>> newHigh
        >>> nearHigh
        >>> nearLow
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/52weeks?regionId=6&rankType={type}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)

        data = await parse_ticker_values(datas)

        if as_dataframe == False:
            return data
        
        df = pd.DataFrame(data)
        df['type'] = type
        df.columns = df.columns.str.lower()
        return df
        
    async def ipos(self, type:str='filing', as_dataframe:bool=True):
        """
        TYPES:

        >>> filing
        >>> pricing
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/ipo/listIpo?regionId=6&status={type}&includeBanner=true"
        datas = await self.fetch_endpoint(endpoint)
        data = await parse_ipo_data(datas)

        if as_dataframe == False:
            return data
        
        df = pd.DataFrame(data)
        df.columns = df.columns.str.lower()


        return df
    

    async def earnings_surprise(self, rank_type):
        """TYPES:
        >>> below - below expectations
        >>> beyond - above expectations
        
        """

        
      
        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/earnings?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=300&order=surpriseRatio&direction=-1")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']
                ticker = [i.get('ticker') for i in data]
                values = [i.get('values') for i in data]

                return EarningSurprise(ticker,values)
            


    async def dividend_yield(self):
        """
        Ranks dividend yields in order from most to least
        
        """

        
      
        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/dividend?regionId=6&rankType=dividend&pageIndex=1&pageSize=300&order=yield&direction=-1")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']
                ticker = [i.get('ticker') for i in data]
                values = [i.get('values') for i in data]

                return Dividend(ticker,values)
            


    async def micro_futures(self, rank_type:str='micro.10Y'):
        """Micro futures!
        
        micro.MYM: Micro E-mini Dow
        micro.MES: Micro E-mini S&P 500
        micro.MNQ: Micro E-mini Nasdaq-100
        micro.M2K: Micro E-mini Russell 2000
        micro.M6A: Micro AUD/USD
        micro.M6B: Micro GBP/USD
        micro.MCD: Micro CAD/USD
        micro.M6E: Micro EUR/USD
        micro.MSF: Micro CHF/USD
        micro.MGC: Micro Gold
        micro.MHG: Micro Copper
        micro.SIL: Micro Silver
        micro.MCL: Micro WTI Crude Oil
        micro.10Y: Micro 10-Year Yield
        micro.30Y: Micro 30-Year Yield
        micro.2YY: Micro 2-Year Yield
        micro.5YY: Micro 5-Year Yield
        micro.MBT: Micro Bitcoin
        micro.MET: Micro Ether
        
        
        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            


    async def equity_index_futures(self, rank_type:str='equityIndex.YM'):
        """Equity index futures!
        
        "equityIndex.YM": "E-mini Dow",
        "equityIndex.NQ": "E-mini Nasdaq",
        "equityIndex.ES": "E-mini S&P",
        "equityIndex.RTY": "E-mini Russell 2000",
        "equityIndex.EMD": "E-mini S&P MidCap 400",
        "equityIndex.MES": "Micro E-mini S&P 500",
        "equityIndex.MNQ": "Micro E-mini Nasdaq-100",
        "equityIndex.M2K": "Micro E-mini Russell 2000",
        "equityIndex.MYM": "Micro E-mini Dow"
            
        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            

    async def fx_futures(self, rank_type:str='fx.6E'):
        """FX futures!
            "fx.6E": "EUR/USD",
            "fx.6J": "JPY/USD",
            "fx.6B": "GBP/USD",
            "fx.6A": "AUD/USD",
            "fx.M6A": "Micro AUD/USD",
            "fx.M6B": "Micro GBP/USD",
            "fx.6C": "CAD/USD",
            "fx.MCD": "Micro CAD/USD",
            "fx.E7": "E-mini EUR/USD",
            "fx.M6E": "Micro EUR/USD",
            "fx.J7": "E-mini JPY/USD",
            "fx.6S": "CHF/USD",
            "fx.MSF": "Micro CHF/USD"
            
        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)

    async def metal_futures(self, rank_type:str='metal.GC'):
        """metal futures!
            "metal.GC": "Gold",
            "metal.SI": "Silver",
            "metal.PL": "Platinum",
            "metal.QO": "E-mini Gold",
            "metal.MGC": "Micro Gold",
            "metal.HG": "Copper",
            "metal.MHG": "Micro Copper",
            "metal.QC": "E-mini Copper",
            "metal.SIL": "Micro Silver",
            "metal.QI": "E-mini Silver",
            "metal.PA": "Palladium"
            
        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            

    async def energy_futures(self, rank_type:str='energy.CL'):
        """energy futures!

            "energy.CL": "Crude Oil",
            "energy.NG": "Natural Gas (Henry Hub)",
            "energy.RB": "RBOB Gasoline",
            "energy.BZ": "Brent Crude Oil",
            "energy.QM": "E-mini Crude Oil",
            "energy.MCL": "Micro WTI Crude Oil",
            "energy.HO": "NY Harbor ULSD",
            "energy.QG": "E-mini Natural Gas"
        
        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            


    async def interest_rate_futures(self, rank_type:str='interestRate.ZN'):
        """interest rate futures!


        "interestRate.ZT": "2-Year T-Note",
        "interestRate.ZF": "5-Year T-Note",
        "interestRate.ZN": "10-Year T-Note",
        "interestRate.TN": "Ultra 10-Year T-Note",
        "interestRate.10Y": "Micro 10-Year Yield",
        "interestRate.UB": "Ultra T-Bond",
        "interestRate.ZB": "U.S. Treasury Bond",
        "interestRate.30Y": "Micro 30-Year Yield",
        "interestRate.2YY": "Micro 2-Year Yield",
        "interestRate.5YY": "Micro 5-Year Yield"

        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            

    async def aggricultural_futures(self, rank_type:str='agricultural.LE'):
        """aggriculture futures!

        "agricultural.ZS": "Soybeans",
        "agricultural.ZW": "Chicago Wheat",
        "agricultural.ZC": "Corn",
        "agricultural.GF": "Feeder Cattle",
        "agricultural.HE": "Lean Hogs",
        "agricultural.LE": "Live Cattle",
        "agricultural.XC": "Mini-Corn",
        "agricultural.XK": "Mini Soybean",
        "agricultural.ZL": "Soybean Oil",
        "agricultural.ZM": "Soybean Meal",
        "agricultural.ZO": "Oats",
        "agricultural.XW": "Mini-sized Chicago Wheat"


        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            

    async def crypto_futures(self, rank_type:str='cryptocurrency.BTC'):
        """crypto futures!


        "cryptocurrency.BTC": "Bitcoin",
        "cryptocurrency.MBT": "Micro Bitcoin",
        "cryptocurrency.ETH": "Ether",
        "cryptocurrency.MET": "Micro Ether"


        """


        async with httpx.AsyncClient() as client:
            data = await client.get(f"https://quotes-gw.webullfintech.com/api/wlas/ranking/futures?rankType={rank_type}&regionId=6&brokerId=8")

            if data.status_code == 200:
                data = data.json()
                data =  data['data']

                values = [i.get('values') for i in data]

                futures = [i.get('futures') for i in data]

                return MicroFutures(futures, values)
            

    async def treasury_tickers(self):
        """get treasury tickers"""

        async with httpx.AsyncClient() as client:
            data = await client.get("https://quotes-gw.webullfintech.com/api/wlas/bonds/list?regionId=6&pageIndex=1&pageSize=200")
            if data.status_code == 200:
                data = data.json()

                data = data['data']
                return TreasuryData(data)
            
    async def fear_greed_index(self):
        """Get fear/greed index and convert timestamps to Eastern Time."""
        async with httpx.AsyncClient() as client:
            # Fetch the data
            response = await client.get(
                "https://uswm.webullfinance.com/api/wealth/v1/wm-strategy/query_history_fear_greed_index"
            )
            data = response.json()

            # Parse different time ranges
            time_ranges = {
                "one_month": data.get("oneMonth", []),
                "three_month": data.get("threeMonth", []),
                "six_month": data.get("sixMonth", []),
                "one_year": data.get("oneYear", []),
            }

            # Timezone for conversion
            eastern = pytz.timezone("US/Eastern")

            # Process each time range into a DataFrame
            processed_data = {}
            for key, records in time_ranges.items():
                if records:
                    df = pd.DataFrame(records)

                    # Convert timestamps to Eastern Time
                    if "timestamp" in df.columns:
                        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")  # Convert to datetime
                        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC").dt.tz_convert(eastern)

                    processed_data[key] = df
                    print(f"Processed DataFrame for {key}:")
                    print(df)

            # Return all processed DataFrames
            return processed_data
        

    def advanced_plot_fear_greed(
        self,
        processed_data,
        time_ranges=None,
        output_path="fear_greed_index_advanced_plot.png",
        dpi=300
    ):
        if time_ranges is None:
            time_ranges = list(processed_data.keys())  # Plot all time ranges by default

        plt.style.use("seaborn-v0_8-darkgrid")
        fig, ax = plt.subplots(figsize=(14, 8))

        color_palette = ['#FF5733', '#33FF57', '#3357FF', '#FF33A6']
        markers = ['o', 's', 'D', '^']

        for idx, time_range in enumerate(time_ranges):
            if time_range in processed_data:
                df = processed_data[time_range]

                # Ensure 'timestamp' is in datetime format
                if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

                # Convert 'fearGreedIndex' to numeric, coerce errors to NaN
                df['fearGreedIndex'] = pd.to_numeric(df['fearGreedIndex'], errors='coerce')

                # Drop rows with invalid data (NaN values in key columns)
                df = df.dropna(subset=['timestamp', 'fearGreedIndex'])

                # Resample or downsample for long time ranges
                if time_range == "one_year":
                    df = df.set_index('timestamp').resample('M').mean().reset_index()  # Monthly average for `one_year`
                elif time_range == "six_month":
                    df = df.set_index('timestamp').resample('W').mean().reset_index()  # Weekly average for `six_month`

                # Plot main trend line
                ax.plot(
                    df["timestamp"],
                    df["fearGreedIndex"],
                    marker=markers[idx % len(markers)],
                    linestyle='-',
                    linewidth=1.5,
                    markersize=5,
                    color=color_palette[idx % len(color_palette)],
                    label=f"{time_range.replace('_', ' ').title()} (Trend)"
                )

                # Smoothed trendline with larger window for long ranges
                if time_range in ["six_month", "one_year"]:
                    window = 30 if time_range == "one_year" else 10
                    ax.plot(
                        df["timestamp"],
                        df["fearGreedIndex"].rolling(window=window, min_periods=1).mean(),
                        linestyle='--',
                        color=color_palette[idx % len(color_palette)],
                        alpha=0.5,
                        label=f"{time_range.replace('_', ' ').title()} (Smoothed)"
                    )

        # Highlight fear and greed zones with background colors
        ax.axhspan(0, 25, color="red", alpha=0.15, label="Extreme Fear Zone")
        ax.axhspan(25, 50, color="orange", alpha=0.15, label="Fear Zone")
        ax.axhspan(50, 75, color="lightgreen", alpha=0.15, label="Greed Zone")
        ax.axhspan(75, 100, color="green", alpha=0.15, label="Extreme Greed Zone")

        # Format x-axis dates
        date_formatter = DateFormatter("%b %Y")
        ax.xaxis.set_major_formatter(date_formatter)
        plt.xticks(rotation=45)

        # Labels and title
        ax.set_title("Fear and Greed Index with Smoothed Trends", fontsize=18, fontweight='bold')
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Fear and Greed Index", fontsize=12)

        # Legend outside the plot
        ax.legend(fontsize=10, loc="upper left", bbox_to_anchor=(1.05, 1), borderaxespad=0.)
        
        # Reduce the density of y-ticks
        ax.set_yticks(range(0, 101, 25))

        # Save and show plot
        plt.tight_layout()
        plt.savefig(output_path, dpi=dpi, bbox_inches='tight')
        print(f"Plot saved as {output_path}")
        plt.show()



    async def us_treasuries(self, treasury_id:str='310000003'):

        url = f"https://quotes-gw.webullfintech.com/api/bonds/realtime/query?tickerId={treasury_id}&more=1"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)

            data = data.json()

            return US_TREASURIES(data)
                


    async def economic_events(self, start_date:str=None, end_date:str=None, type:str='1', page_size:str='100'):
        """Get upcoming economic events."""
        if start_date == None:
            start_date = self.today

        if end_date == None:
            end_date = self.thirty_days_from_now


        url = f"https://quotes-gw.webullfintech.com/api/market/calendar/economic?pageSize={page_size}&startDate={start_date}&endDate={end_date}&pageIndex=1&regionIds=6%2C1%2C2%2C13%2C18%2C159&types=2&timeZone=America%2FNew_York"


        async with httpx.AsyncClient() as client:
            data = await client.get(url)

            data = data.json()
    

            return EconEvents(data)
        


    async def econ_data(self, start_date:str=None, end_date:str=None, page_index:str='1', type:str='1', db=None):
        if start_date == None:
            start_date = self.today
        if end_date == None:
            end_date = self.thirty_days_from_now

        economic_data=f"https://quotes-gw.webullfintech.com/api/market/calendar/economic?pageSize=200&startDate={start_date}&endDate={end_date}&pageIndex={page_index}&regionIds=6%2C1%2C2%2C13%2C18%2C159&types={type}&timeZone=America%2FNew_York"


        async with httpx.AsyncClient() as client:
            data = await client.get(economic_data)

            data = data.json()
            data = Econ(data)
            
            
            if db is not None:
                await db.connect()
                await db.batch_upsert_dataframe(data.as_dataframe, table_name='econ', unique_columns=['def_id', 'src_id'])
    

            return data


    async def earnings_surprises(rank_type:str='below', db=None):


        url = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/earnings?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=50&order=surpriseRatio&direction=-1"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)
            data = data.json()
            data = data['data']

            ticker = [i.get('ticker') for i in data]
            values = [i.get('values') for i in data]

            earnings_data = EarningsSurprises(ticker, values)

            earnings_data.as_dataframe['rank_type'] = rank_type
            if db is not None:
                await db.connect()
                await db.batch_upsert_dataframe(earnings_data.as_dataframe, table_name='er_surprises', unique_columns=['symbol', 'release_date'])

            return earnings_data

    async def alerts(self, db=None, headers=None):
        
        url = f"https://quotes-gw.webullfintech.com//api/wlas/portfolio/changes?supportBroker=8"
        payload = {"tickerIds":[913255395,925179279,913323808,913255053,950979972,950118834,913255341,913246499,913256043,950188842,913324348,913323867,950118597,913255073,913243155,913256732,913254910,913324002,913255353,913256060,913256863,913255659,913243857,913254850,913324439,913254990,913323987,913243989,913323778,913244915,913324681,913255309,913255942,913255007,913324489,950116149,913255443,913255369,913255501,913255655,913246633,913255171,950187715,913243128,950118416,913256036,913324495,950052430,925377978,950300038,950977519,950182845,913255124,913243231,950186258,913324101,950145440,913255467,913243223,913255864,913244089,925242603,950172303,950052391,913424717,913323997,950174696,913255044,913433908,913254759,913323516,913255003,913255192,913324095,913324337,950064710,950989569,950118595,913247475,913323999,913323281,925377113,913255489,913243232,913255471,916040631,950171618,913254447,913324084,916040735,913243750,913323815,913257562,913323796,913323901,913324571,916040682,913255382,913323450,913323857,913255105,913323878,913243250,913246631,913324008,913244503,950153166,913255414,950173560,913243251,913256135,913255062,913323554,913243154,913323571,913256180,913255270,950178075,913254872,950138392,950102542,950979523,913324222,913253402,913246449,913255255,913256167,913431510,913243139,913353636,913255508,913243883,950133162,913255936,913324267,950062937,950126602,913255002,913430648,913257501,913256740,913242920,913323915,913257568,913323953,950127428,950102389,913247534,913323709,913324421,913255163,913254735,913244138,913255114,913324459,913323591,950169475,913255373,913324083,950131948,913244725,913255078,913324577,950188536,950165760,913255803,913323300,913247095,913255509,913244722,913255289,913256320,913254903,913256616,913254895,913254235,913246740,913243584,913730084,913324268,913424716,913243145,913323511,913255033,913244105,913257472,950178653,950172451,950988322,913256217,913254831,913243611,913255275,916040691,913243581,913255030,913254936,913247485,950052449,913255102,913254222,913254798,913324070,913247399,913433476,950178170,913256091,913324503,916040647,913257299,913243249,913256042,913255197,913324096,925179282,913324165,913243229,925418532,913243088,913255122,913303964,950121423,913243233,913244540,913256419,913324114,913256072,913254998,950180986,913255505,950102408,950169866,913244665,913254473,913323809,913323750,913257561,913255347,950187944,913244550,913256357,913246626,913255140,925163605,925241477,950186274,913324679,913256192,913255162,913255266,916040747,950076017,950091058,913255286,913255253,913255490,950178463,913254679,950136918,925299128,913254330,950979350,913245193,916040650,913323925,925186755,913243612,913254427,913324504,916040709,950136998,913324497,913255852,913324520,913244548,913323566,950052670,925376726,913243296,913255618,913323523,913256303,913255495,913253512,913254999,913246554,913324397,913255151,913255598,950125723,950169649,913255455,913243104,913243879,913323879,950178219,950153159,913257027,925241196,913324100,950181408,913732468,913354088,913254303,913254883,913323816,913255679,913255666,916040734,913256672,925415331,913324585,913247474,950173437,913255218,925415088], "sId": 0, "limit": 150}
        async with httpx.AsyncClient(headers=headers) as client:
            data = await client.post(url, json=payload)
            data = data.json()




            alert_data = Alerts(data)
            if db is not None:
                await db.connect()
                await db.batch_upsert_dataframe(alert_data.as_dataframe, table_name='alerts', unique_columns=['ticker', 'alert_type'])
            return alert_data


    async def tc_summaries(self, type:str='1', time_horizon:str='Middle', page_size:str='100'):
        """
        bullish type = 1
        bearish type = 2

        time horizons:
            >>> Middle
            >>> Long
            >>> Short

        """

        url = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/tc-rank?regionId=6&supportBroker=8&type=1&rankType=technicalEvents.tc{time_horizon}&pageIndex=1&pageSize={page_size}"

        async with httpx.AsyncClient() as client:
            data = await client.get(url)

            data = data.json()
            data = data['data']

            ticker = [i.get('ticker') for i in data]
            values = [i.get('values') for i in data]

            data = TCSummaries(ticker, values)

            return data