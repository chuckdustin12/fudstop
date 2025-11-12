import pandas as pd
import asyncio
import aiohttp
from fudstop4.apis.polygonio.polygon_options import PolygonOptions
db = PolygonOptions()




class VlexSDK:
    def __init__(self, cookie:str):
        self.cookie=cookie




    async def get_user_history(self):
        url = f"https://us-vincent.vlex.com/user_history/"


        async with aiohttp.ClientSession(headers={'cookie': self.cookie}) as session:
            async with session.get(url) as response:
                data = await response.json()
                return data