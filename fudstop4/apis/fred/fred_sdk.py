from fudstop4.apis.polygonio.polygon_options import PolygonOptions
import os
from dotenv import load_dotenv
load_dotenv()
opts = PolygonOptions()

import pandas as pd


import aiohttp



import asyncio





class fredSDK:
    def __init__(self):
        self.key= os.environ.get('YOUR_FRED_KEY')




    async def releases(self):

        async with aiohttp.ClientSession() as session:

            async with session.get(f"https://api.stlouisfed.org/fred/releases?api_key={self.key}&file_type=json") as resp:

                data = await resp.json()
                releases = data['releases']
                df = pd.DataFrame(releases)



            return df
        

    async def release_dates(self):

        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.stlouisfed.org/fred/releases/dates?api_key={self.key}&file_type=json") as resp:


                data = await resp.json()


                release_dates = data['release_dates']


                df = pd.DataFrame(release_dates)

                return df