import os
import aiohttp
import asyncio
import pandas as pd
from dotenv import load_dotenv
from .models import Gex,GexMajorLevels,MaxGex


load_dotenv()

from typing import List

class GEXBot:
    def __init__(self):
        self.key = os.environ.get('GEXBOT')
        self.tickers = ["SPY","QQQ","AAPL","TSLA","MSFT","AMZN","NVDA"]
        self.df = None



    async def get_gex(self, ticker:str, as_dataframe:bool = True):
        """
        Gets GEX levels for a ticker.

        Arguments:


        >>> ticker: the ticker to survey (REQUIRED)
        
        >>> as_dataframe: returns as a dataframe (optional - default True)

        >>> concurrent: - optional - runs function for all tickers (default False)
        """
        url=f"https://api.gexbot.com/{ticker}/gex/all?key={self.key}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                if data is not None:
                    if as_dataframe == False:
                        return Gex(data)
                    data = Gex(data).as_dataframe
                   
                    return data
  
    async def major_levels(self, ticker, as_dataframe:bool=True):
        """
        Gets major gex levels

        Arguments:


        >>> ticker: required - the ticker to survey

        >>> as_dataframe: optional - returns as dataframe (default True)

        
        
        """
        url = f"https://api.gexbot.com/{ticker}/gex/all/majors?key={self.key}"
       
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                if data is not None:
                    if as_dataframe == False:
                        return GexMajorLevels(data)
                    
                    try:
                        # Attempt to use the as_dataframe attribute
                        dataframe = GexMajorLevels(data).as_dataframe
                        return dataframe
                    except AttributeError:
                        # Skip if the attribute does not exist
                        print(f'Error - {ticker}')
                
                

    async def max_gex(self,ticker, as_dataframe: bool=True):
        """Gets MAX gex levels
        
        Arguments:

        >>> ticker - required - the ticker to survey

        >>> as_dataframe - optional - returns as a dataframe (default True)
        

        """

        url = f"https://api.gexbot.com/{ticker}/gex/all/maxchange?key={self.key}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                if data is not None:
                    if as_dataframe == False:
                        return MaxGex(data)
                    return MaxGex(data).as_dataframe


    async def monitor_all_gex(self, ticker) -> List[Gex]:
        url = f"https://api.gexbot.com/{ticker}/gex/all?key={self.key}"
        all_data_dicts = []
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                if data is not None:
                    data = Gex(data)
                    all_data_dicts.append(data.data_dict)

        return all_data_dicts

    async def run_all_gex(self):
        tasks = [self.monitor_all_gex(i) for i in self.tickers]
        results = await asyncio.gather(*tasks)

        # Convert each list to a DataFrame before concatenation
        dataframes = [pd.DataFrame(r) for r in results if isinstance(r, list)]
        df = pd.concat(dataframes, ignore_index=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        # Assuming df['timestamp'] is already in UTC
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC').dt.tz_convert('US/Eastern')

        # Remove timezone information
        df['timestamp'] = df['timestamp'].dt.tz_localize(None)
        

        return df