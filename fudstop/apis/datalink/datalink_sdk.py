import os
import sys
from pathlib import Path
from .models import RetailActivity, OptionsRank
import json
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from dotenv import load_dotenv
load_dotenv()
import asyncio
from datetime import datetime, timedelta
from _asyncpg.asyncpg_sdk import AsyncpgSDK
import pandas as pd
import httpx
class DataLinkSDK:
    def __init__(self):
        self.key = os.environ.get('YOUR_NASDAQ_KEY')

        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.db = AsyncpgSDK()

    async def option_rating(self, ticker:str, date:str=None):
        if date == None:
            date = self.yesterday

        endpoint = f"https://data.nasdaq.com/api/v3/datatables/QUANTCHA/QOR?date={date}&ticker={ticker}&api_key={self.key}"


        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)
            datas = data.json()

            # Assuming 'datas' is your input data structure
            datatable = datas['datatable'] if 'datatable' in datas else {}
            data = datatable.get('data', [])
            datas = OptionsRank(data)
      
            columns = datatable.get('columns', [])
            column_names = [column.get('name') for column in columns]
            print(column_names)

            # Create the DataFrame
            df = pd.DataFrame(data, columns=column_names)
            await self.db.connect()
            await self.db.batch_insert_dataframe(df, table_name='ivcrush', unique_columns='ticker, date')
       
            
            return datas
           
        

    async def retail_activity(self, ticker:str, date:str=None):
        await self.db.connect()
        if date == None:
            date = self.today
        endpoint= f"https://data.nasdaq.com/api/v3/datatables/NDAQ/RTAT10?date={date}&ticker={ticker}&api_key={self.key}"
        print(endpoint)
        async with httpx.AsyncClient() as client:
            data = await client.get(endpoint)

            data = data.json()

            datatable = data['datatable']
            data = datatable['data']

            data = RetailActivity(data)

            await self.db.batch_insert_dataframe(data.as_dataframe, table_name='retail_activity', unique_columns='ticker, date')

            return data
        
            


