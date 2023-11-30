import os
connection_string = os.environ.get('CONNECTION_STRING')
import aiohttp
import asyncio
import pandas as pd
from pytz import timezone
from .database_manager import DatabaseManager

from .webull_helpers import parse_most_active, parse_total_top_options, parse_contract_top_options, parse_ticker_values, parse_ipo_data, parse_etfs

class WebullMarkets(DatabaseManager):
    """General market data from webull"""
    def __init__(self, connection_string=None, pool=None):

        self.pool = pool
        self.connection_string = connection_string
        self.most_active_types = ['rvol10d', 'turnoverRatio', 'volume', 'range']
        self.top_option_types = ['totalVolume', 'totalPosition', 'volume', 'position', 'impVol', 'turnover']
        self.top_gainer_loser_types = ['afterMarket', 'preMarket', '5min', '1d', '5d', '1m', '3m', '52w']
        self.etf_types = ['industry', 'index', 'commodity', 'other']
        self.high_and_low_types = ['newHigh', 'newLow', 'nearHigh', 'nearLow']

       

        self.headers = {
        "App": "global",
        'Access-Token': os.environ.get('webull_access_token'),
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
        "Reqid": os.environ.get('REQ_ID'),
        "Sec-Ch-Ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "T_time": "1698276695206",
        "Tz": "America/Los_Angeles",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ver": "3.40.11",
        "X-S": os.environ.get('YOUR_X-S'),
        "X-Sv": "xodp2vg9"
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
        df.to_csv(f'data/top_options/top_options_{rank_type}.csv', index=False)
        df.columns = df.columns.str.lower()
        df = df.drop(columns=['dt', 'sectype', 'fatradetime', 'tradetime', 'status', 'template'])
        if self.connection_string is not None:
            df['totalasset'] = df['totalasset'].astype(float)
            df['netasset'] = df['netasset'].astype(float)
            df['implvol'] = df['implvol'].astype(float)
            df['position'] = df['position'].astype(float)
            df['middleprice'] = df['middleprice'].astype(float)
            df['turnover'] = df['turnover'].astype(float)
            df['positionchange'] = df['positionchange'].astype(float)
            df['unsymbol'] = df['unsymbol'].astype('string')
            df['strikeprice'] = df['strikeprice'].astype(float)
            df['price'] = df['price'].astype(float)
            df['direction'] = df['direction'].astype('string')
            # Convert columns to float
            float_columns = ['close', 'change', 'changeratio', 'marketvalue', 'volume', 'turnoverrate',
                            'pettm', 'preclose', 'fiftytwowkhigh', 'fiftytwowklow', 'open', 'high', 
                            'low', 'vibrateratio', 'pchratio', 'pprice', 'pchange']
            df[float_columns] = df[float_columns].astype(float)

    
          
     

  
 
            await self.connect()
            await self.batch_insert_dataframe(df, table_name=f'top_options_{rank_type.lower()}', unique_columns='insertion_timestamp')

        
        return df


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
        if self.connection_string is not None:

            await self.connect()
            await self.batch_insert_dataframe(df, table_name=f'most_active_{rank_type}', unique_columns='insertion_timestamp')
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


    async def earnings(self, start_date:str, pageSize: str='100', as_dataframe:str=True):
        """
        Pulls a list of earnings.

        >>> Start Date: enter a start date in YYYY-MM-DD format.

        >>> pageSize: enter the amount to be returned. default = 100

        >>> as_dataframe: default returns as a pandas dataframe.
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/explore/calendar/earnings?regionId=6&pageIndex=1&pageSize={pageSize}&startDate={start_date}"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_ticker_values(datas)
        if as_dataframe == False:
            return parsed_data
       
        df = pd.DataFrame(parsed_data)
        df.columns = df.columns.str.lower()
        df.to_csv('data/earnings/earnings_upcoming.csv', index=False)
        df = df.rename(columns={'v_releasedate': 'release_date'})
        if self.connection_string is not None:
            await self.connect()
            await self.batch_insert_dataframe(df, table_name='earnings', unique_columns='release_date')

        return df


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
        df.to_csv(f'data/top_gainers/top_gainers_{rank_type}.csv', index=False)
        if self.connection_string is not None:
            if 't_sectype' in df.columns:
                df = df.drop(columns=['t_sectype'])
            await self.connect()
            await self.batch_insert_dataframe(df, table_name=f'top_gainers_{rank_type}', unique_columns='insertion_timestamp')
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

        df.to_csv(f'data/top_losers/top_losers{rank_type}.csv', index=False)
        df.columns = df.columns.str.lower()
        if self.connection_string is not None:
            if 't_sectype' in df.columns:
                df.drop(['t_sectype'], axis=1, inplace=True)
            await self.connect()
            await self.batch_insert_dataframe(df, table_name=f'top_losers_{rank_type}', unique_columns='insertion_timestamp')
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

        df.to_csv('data/forex/forex_quotes.csv', index=False)
        df.columns = df.columns.str.lower()
        if self.connection_string is not None:
            if 't_sectype' in df.columns:
                df.drop(['t_sectype'], axis=1, inplace=True)
            await self.connect()
            await self.batch_insert_dataframe(df, table_name='forex', unique_columns='insertion_timestamp')
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
        df.to_csv(f'data/etfs/etfs_{type}.csv', index=False)
        df.columns = df.columns.str.lower()
        if self.connection_string is not None:
            df = df.drop(columns=['id', 'sectype', 'exchangetrade'])
            await self.connect()
            await self.batch_insert_dataframe(df, table_name=f'etfs_{type}', unique_columns='insertion_timestamp')

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
        df.to_csv(f'data/highs_and_lows/{type}.csv', index=False)
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
        df.to_csv(f'data/ipos/ipo_list_{type}.csv')

        return df
    

    